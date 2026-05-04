sudo echo "=== Configuration ==="
echo ""

echo "??? Please enter your TorBox API Key found on https://torbox.app/settings ???"
read -r api_key

clear

if [ -z "$api_key" ]; then
    echo "!!! API Key is required. !!!"
    exit 1
fi

echo "??? Would you like to run TorBox Media Center as Fuse or Strm (fuse/strm) ???"
read -r run_as

if [ "$run_as" != "fuse" ] && [ "$run_as" != "strm" ]; then
    echo "!!! Invalid input. Please enter fuse or strm. !!!"
    exit 1
fi

clear

current_user=$(id -un)

echo "??? Where would you like the files to be stored ???"
echo "--- Default: /Users/$current_user/torbox-media-center ---"
read -r storage_path

if [ -z "$storage_path" ]; then
    storage_path="/Users/$current_user/torbox-media-center"
else
    storage_path="$storage_path"
fi

clear

echo "=== Checking dependencies ==="
echo ""
# check if Docker and Fuse are installed
if ! command -v docker &> /dev/null; then
    echo "!!! Docker is not installed or not in the $PATH. Please install Docker and try again. !!!"
    exit 1
else
    echo "--- Found Docker. Checking if Docker is running... ---" 
    if ! docker info &> /dev/null; then
        echo "!!! Docker is not running. Please start Docker and try again. !!!"
        exit 1
    else
        echo "--- Docker is running. ---"
    fi
fi

# check if Fuse is installed only if run_as is fuse
if [ "$run_as" == "fuse" ]; then
    if ! command -v fuse &> /dev/null; then
        echo "!!! Fuse is not installed. Please install Fuse and try again. !!!"
        exit 1
    else
        echo "--- Fuse is installed. ---"
    fi
fi

clear

echo "=== Creating directories ==="
echo ""
mkdir -p "$storage_path"
echo "--- Created directory: $storage_path ---"

clear

echo "=== Running TorBox Media Center ==="
echo ""

sudo docker run -it -d --name=torbox-media-center --restart=always --init \
    -v "$storage_path:/torbox" \
    -e TORBOX_API_KEY="$api_key" \
    -e MOUNT_METHOD="$run_as" \
    -e MOUNT_PATH="/torbox" \
    anonymoussystems/torbox-media-center:latest

clear

echo "=== TorBox Media Center is now running! ==="
echo ""
echo "--- Mount Path: $storage_path ---"
echo "--- Run as: $run_as ---"
echo "--- API Key: $api_key ---"
echo "-- To view logs run: sudo docker logs -f torbox-media-center"
echo "-- To restart the container run: sudo docker restart torbox-media-center"
echo "-- To stop the container run: sudo docker stop torbox-media-center"
echo "-- To remove the container run: sudo docker rm torbox-media-center"
