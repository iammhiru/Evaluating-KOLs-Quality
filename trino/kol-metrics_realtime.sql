CREATE OR REPLACE VIEW iceberg.db1.kol_metrics_realtime AS

WITH
  -- 1) Dedup & aggregate posts (loại 'post')
  posts_latest AS (
    SELECT
      page_id,
      post_id,
      type,
      total_comment,
      total_share,
      total_view,
      total_emotes,
      likes,
      loves,
      wow,
      care,
      haha,
      record_ts,
      ROW_NUMBER() OVER (
        PARTITION BY page_id, post_id
        ORDER BY record_ts DESC
      ) AS rn
    FROM iceberg.db1.kol_post_stream
  ),

  posts_dedup AS (
    SELECT
      *
    FROM posts_latest
    WHERE rn = 1
  ),

  posts_agg AS (
    SELECT
      page_id,
      COUNT(*)                                           AS post_count,
      ROUND(AVG(total_comment + total_share + total_emotes), 4)
                                                        AS engagement_per_post,
      ROUND(AVG(likes + loves + wow + care + haha), 4)  AS applause_per_post,
      ROUND(AVG(total_comment), 4)                       AS conversion_per_post,
      ROUND(AVG(total_share), 4)                         AS amplification_per_post
    FROM posts_dedup
    WHERE lower(type) = 'post'
    GROUP BY page_id
  ),

  -- 2) Dedup & aggregate videos (loại 'video'), round bên trong AVG
  videos_agg AS (
    SELECT
      page_id,
      COUNT(*)                                              AS video_count,
      AVG(total_comment + total_share + total_emotes)       AS engagement_per_video,
      ROUND(AVG(total_view), 4)                             AS views_per_video,
      AVG(likes + loves + wow + care + haha)                AS applause_per_video,
      AVG(total_comment)                                     AS conversion_per_video,
      AVG(total_share)                                       AS amplification_per_video,
      AVG(
        (likes + loves + wow + care + haha) * 100.000000
        / NULLIF(total_view, 0)
      )                                                      AS applause_video_rate,
      AVG(
        total_comment * 100.000000
        / NULLIF(total_view, 0)
      )                                                      AS conversion_video_rate,
      AVG(
        (total_comment + total_share + total_emotes) * 100.000000
        / NULLIF(total_view, 0)
      )                                                      AS engagement_to_view_video_rate
    FROM posts_dedup
    WHERE lower(type) = 'video'
    GROUP BY page_id
  ),

  -- 3) Dedup & aggregate reels, tương tự
  reels_latest AS (
    SELECT
      page_id,
      reel_id,
      views,
      comments,
      likes,
      share,
      record_ts,
      ROW_NUMBER() OVER (
        PARTITION BY page_id, reel_id
        ORDER BY record_ts DESC
      ) AS rn
    FROM iceberg.db1.kol_reel_stream
  ),

  reels_dedup AS (
    SELECT
      *
    FROM reels_latest
    WHERE rn = 1
  ),

  reels_agg AS (
    SELECT
      page_id,
      COUNT(*)                                             AS reel_count,
      ROUND(AVG(views), 4)                                 AS views_per_reel,
      AVG(comments + likes + share)                        AS engagement_per_reel,
      AVG(likes)                                           AS applause_per_reel,
      AVG(comments)                                        AS conversion_per_reel,
      AVG(share)                                           AS amplification_per_reel,
      AVG(
        (comments + likes + share) * 100.000000
        / NULLIF(views, 0)
      )                                                     AS engagement_to_view_reel_rate,
      AVG(
        likes * 100.000000
        / NULLIF(views, 0)
      )                                                     AS applause_reel_rate,
      AVG(
        comments * 100.000000
        / NULLIF(views, 0)
      )                                                     AS conversion_reel_rate,
      AVG(
        share * 100.000000
        / NULLIF(views, 0)
      )                                                     AS amplification_reel_rate
    FROM reels_dedup
    GROUP BY page_id
  )

-- 4) Final join với profile và normalize theo followers
SELECT
  p.page_id,
  p.name,
  p.category,
  p.followers_count,

  -- Posts
  COALESCE(pa.post_count, 0)                                     AS post_count,
  COALESCE(
    pa.engagement_per_post * 100.0 / NULLIF(p.followers_count, 0),
    0
  )                                                               AS engagement_post_rate,
  COALESCE(
    pa.applause_per_post * 100.0 / NULLIF(p.followers_count, 0),
    0
  )                                                               AS applause_post_rate,
  COALESCE(
    pa.conversion_per_post * 100.0 / NULLIF(p.followers_count, 0),
    0
  )                                                               AS conversion_post_rate,
  COALESCE(
    pa.amplification_per_post * 100.0 / NULLIF(p.followers_count, 0),
    0
  )                                                               AS amplification_post_rate,

  -- Videos
  COALESCE(va.video_count, 0)                                     AS video_count,
  COALESCE(va.views_per_video, 0)                                 AS views_per_video,
  COALESCE(va.applause_video_rate, 0.0)                           AS applause_video_rate,
  COALESCE(va.conversion_video_rate, 0.0)                         AS conversion_video_rate,
  COALESCE(va.engagement_to_view_video_rate, 0.0)                 AS engagement_to_view_video_rate,

  -- Reels
  COALESCE(ra.reel_count, 0)                                      AS reel_count,
  COALESCE(ra.views_per_reel, 0)                                  AS views_per_reel,
  COALESCE(ra.engagement_to_view_reel_rate, 0.0)                  AS engagement_to_view_reel_rate,
  COALESCE(ra.applause_reel_rate, 0.0)                            AS applause_reel_rate,
  COALESCE(ra.conversion_reel_rate, 0.0)                          AS conversion_reel_rate,
  COALESCE(ra.amplification_reel_rate, 0.0)                       AS amplification_reel_rate,

  -- Overall metrics
  COALESCE(
    (
      COALESCE(pa.post_count        * pa.engagement_per_post, 0)
      + COALESCE(va.video_count      * va.engagement_per_video, 0)
      + COALESCE(ra.reel_count       * ra.engagement_per_reel, 0)
    ) * 100.000000 / p.followers_count
    / NULLIF(
        COALESCE(pa.post_count, 0)
        + COALESCE(va.video_count, 0)
        + COALESCE(ra.reel_count, 0),
        0
      ),
    0
  )                                                               AS overall_engagement_rate,
  COALESCE(
    (
      COALESCE(pa.post_count        * pa.applause_per_post, 0)
      + COALESCE(va.video_count      * va.applause_per_video, 0)
      + COALESCE(ra.reel_count       * ra.applause_per_reel, 0)
    ) * 100.000000 / p.followers_count
    / NULLIF(
        COALESCE(pa.post_count, 0)
        + COALESCE(va.video_count, 0)
        + COALESCE(ra.reel_count, 0),
        0
      ),
    0
  )                                                               AS overall_applause_rate,
  COALESCE(
    (
      COALESCE(pa.post_count        * pa.conversion_per_post, 0)
      + COALESCE(va.video_count      * va.conversion_per_video, 0)
      + COALESCE(ra.reel_count       * ra.conversion_per_reel, 0)
    ) * 100.000000 / p.followers_count
    / NULLIF(
        COALESCE(pa.post_count, 0)
        + COALESCE(va.video_count, 0)
        + COALESCE(ra.reel_count, 0),
        0
      ),
    0
  )                                                               AS overall_conversion_rate,
  COALESCE(
    (
      COALESCE(pa.post_count        * pa.amplification_per_post, 0)
      + COALESCE(va.video_count      * va.amplification_per_video, 0)
      + COALESCE(ra.reel_count       * ra.amplification_per_reel, 0)
    ) * 100.000000 / p.followers_count
    / NULLIF(
        COALESCE(pa.post_count, 0)
        + COALESCE(va.video_count, 0)
        + COALESCE(ra.reel_count, 0),
        0
      ),
    0
  )                                                               AS overall_amplification_rate

FROM iceberg.db1.kol_profile_stream AS p
LEFT JOIN posts_agg  AS pa ON p.page_id = pa.page_id
LEFT JOIN videos_agg AS va ON p.page_id = va.page_id
LEFT JOIN reels_agg  AS ra ON p.page_id = ra.page_id;
