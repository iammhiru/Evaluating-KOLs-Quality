import pyautogui

pyautogui.moveTo(1268, 62)  # Di chuột đến điểm đó
while True:
    print(pyautogui.position(), end="\r")
