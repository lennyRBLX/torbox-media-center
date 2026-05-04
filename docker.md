## Converting Commands to Docker Compose
Since Docker compose isn't as popular as Docker run commands, we only supplied the most common one [here](https://github.com/TorBox-App/torbox-media-center/blob/main/docker-compose.yaml), but since there are so many different iterations of the Docker run command, you can convert any Docker run command to a Docker compose file using [Composerize](https://www.composerize.com/), which is a pretty neat tool.

Simply paste in the Docker run command you need and it will accurately and instantly convert it to a Docker compose which you can then use in tools like [Portainer](https://www.portainer.io/), or create your own full compose file which includes Plex, Jellyfin, or Emby!

## Standard Docker Run Command (STRM)

Below is a standard Docker run command if spawning from the command line. Keep in mind the volume mount paths. If you change these in the environment variable, make sure you change it in the volume mount path.

```bash
docker run -it -d \
    --name=torbox-media-center \
    --restart=always \
    --init \
    -v /home/$(whoami)/torbox:/torbox \
    -e TORBOX_API_KEY=<EDIT_THIS_KEY> \
    -e MOUNT_METHOD=strm \
    -e MOUNT_PATH=/torbox \
    anonymoussystems/torbox-media-center:latest
```

## Standard Docker Run Command (FUSE)

Below is a standard Docker run command for using FUSE. Notice the difference when attaching the `/dev/fuse` device. We also needed to add the `--cap-add SYS_ADMIN` parameter. The Docker container needs access to the FUSE device of the system to activate the virtual file system.

> [!CAUTION]
> The `--cap-add SYS_ADMIN` parameter gives the container certain permissions which it would otherwise have. This is required to access the host systems FUSE device, but keep this in mind. You can read more about this [here](https://docs.docker.com/reference/cli/docker/container/run/#privileged).

```bash
docker run -it -d \
    --name=torbox-media-center \
    --restart=always \
    --init \
    --cap-add SYS_ADMIN \
    --device /dev/fuse \
    -v /mnt/torbox:/torbox:rshared \
    -e TORBOX_API_KEY=<EDIT_THIS_KEY> \
    -e MOUNT_METHOD=fuse \
    -e MOUNT_PATH=/torbox \
    anonymoussystems/torbox-media-center:latest
```


## Standard Docker Run Command Using GitHub Repository (STRM)

This config below is exactly the same as the above, except it is pulling from the GitHub repository rather than Docker Hub. Some people, find that GitHub is much faster than Docker, and sometimes Docker is down.
```bash
docker run -it -d \
    --name=torbox-media-center \
    --restart=always \
    --init \
    -v /home/$(whoami)/torbox:/torbox \
    -e TORBOX_API_KEY=<EDIT_THIS_KEY> \
    -e MOUNT_METHOD=strm \
    -e MOUNT_PATH=/torbox \
    ghcr.io/torbox-app/torbox-media-center:latest
```
## Docker Run Command Changing Location Of Files On Local System (STRM)

This config below changes where on the local system the files are mounted. Instead of in the home directory, it is now mounted in the `/mnt/torbox` directory, which is where you would be able to find your mounted files.

```bash
docker run -it -d \
    --name=torbox-media-center \
    --restart=always \
    --init \
    -v /mnt/torbox:/torbox \
    -e TORBOX_API_KEY=<EDIT_THIS_KEY> \
    -e MOUNT_METHOD=strm \
    -e MOUNT_PATH=/torbox \
    anonymoussystems/torbox-media-center:latest
```

## Docker Run Command Changing Location Of Files In Container (STRM)

This config below changes where on the Docker side where the files are stored. This isn't exactly necessary, but some people may want to change it, so here is a sample where the local system sees the mounted files at `/mnt/torbox` but inside the container, the files are now located at `/data`. Keep in mind that the `MOUNT_PATH` changed to reflect this.

```bash
docker run -it -d \
    --name=torbox-media-center \
    --restart=always \
    --init \
    -v /mnt/torbox:/data \
    -e TORBOX_API_KEY=<EDIT_THIS_KEY> \
    -e MOUNT_METHOD=strm \
    -e MOUNT_PATH=/data \
    anonymoussystems/torbox-media-center:latest
```

## Standard Docker Run Command Using The Very Latest Update (STRM)

Below is a standard Docker run command for running the bleeding edge of this repository. This usually isn't a good idea, but can be used for testing the latest updates, or debugging issues. For the latest release updates, use the `latest` Docker image tag. To get the latest nightly/bleeding edge/source updates straight from the repository, use the `main` Docker image tag. You also may use a commit SHA hash to get the image of a specific commit. For example: `sha-bdd4339`. This works with both GitHub and Docker repository URLs.

```bash
docker run -it -d \
    --name=torbox-media-center \
    --restart=always \
    --init \
    -v /home/$(whoami)/torbox:/torbox \
    -e TORBOX_API_KEY=<EDIT_THIS_KEY> \
    -e MOUNT_METHOD=strm \
    -e MOUNT_PATH=/torbox \
    anonymoussystems/torbox-media-center:main
```
