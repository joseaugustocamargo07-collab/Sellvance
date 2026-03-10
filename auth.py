modules = ["python-3.11"]

[nix]
channel = "stable-24_05"

[deployment]
run = ["python", "main.py"]
deploymentTarget = "cloudrun"

[[ports]]
localPort = 8080
externalPort = 80
