import subprocess


class Task:
    @staticmethod
    def edge():
        command = "taskkill /F /IM msedge.exe"
        subprocess.run(command, shell=True)

    @staticmethod
    def firefox():
        command = "taskkill /F /IM firefox.exe"
        subprocess.run(command, shell=True)

    @staticmethod
    def chrome():
        command = "taskkill /F /IM chrome.exe"
        subprocess.run(command, shell=True)
