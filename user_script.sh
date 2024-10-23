echo "[LOGGING] Updating package list"
sudo apt update
echo "[LOGGING] Attempting to install ffmpeg"
sudo apt install ffmpeg
echo "[LOGGING] Printinig ffmpeg version"
ffmpeg -version

