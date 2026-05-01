import os
import glob
import logging
import threading
from library.app import RAW_MODE
from library.filesystem import MOUNT_PATH
from functions.appFunctions import getAllUserDownloads

_runStrm_lock = threading.Lock()


def getMountCategory(media_type: str | None):
    if media_type == "movie":
        return "movies"
    if media_type == "series" or media_type == "anime":
        return "series"
    return None


def generateFolderPath(data: dict) -> str | None:
    """
    Takes in a user download and returns the folder path for the download.
    """

    if RAW_MODE:
        original_path = data.get("path")
        if original_path:
            return os.path.dirname(original_path)
        return None
    else:
        root_folder: str | None = data.get("metadata_rootfoldername", None)
        metadata_foldername: str | None = data.get("metadata_foldername", None)
        media_type = data.get("metadata_mediatype")

        if not root_folder:
            return None

        if media_type == "series" or media_type == "anime":
            if not metadata_foldername or data.get("metadata_episode") is None:
                return None
            return os.path.join(
                root_folder,
                metadata_foldername,
            )

        if media_type == "movie":
            return os.path.join(root_folder)

        return None


def writeStremFile(strm_path: str, urls: list[str]) -> bool:
    if not urls:
        return False
    content = "\n".join(urls)
    try:
        os.makedirs(os.path.dirname(strm_path), exist_ok=True)
        if os.path.exists(strm_path):
            try:
                with open(strm_path, "r") as existing:
                    if existing.read() == content:
                        return True
            except OSError:
                pass
        with open(strm_path, "w") as file:
            file.write(content)
        logging.debug(
            f"Wrote strm file ({len(urls)} url{'s' if len(urls) != 1 else ''}): {strm_path}"
        )
        return True
    except FileNotFoundError as e:
        logging.error(
            f"Error creating strm file (likely bad naming scheme of file): {e}"
        )
        return False
    except OSError as e:
        logging.error(
            f"Error creating strm file (likely bad or missing permissions): {e}"
        )
        return False
    except Exception as e:
        logging.error(f"Error creating strm file: {e}")
        return False


def runStrm():
    if not _runStrm_lock.acquire(blocking=False):
        logging.info("Skipping runStrm because another run is already in progress.")
        return
    try:
        all_downloads = getAllUserDownloads()

        if not all_downloads:
            logging.info(
                "No downloads found in database. Skipping strm sync to avoid deleting existing files."
            )
            return

        # Get all existing .strm files
        existing_strm_files = set(
            glob.glob(os.path.join(MOUNT_PATH, "**", "*.strm"), recursive=True)
        )

        path_to_urls: dict[str, list[str]] = {}
        for download in all_downloads:
            url = download.get("download_link")
            file_name = download.get("metadata_filename")
            if not url or not file_name:
                continue
            file_path = generateFolderPath(download)
            if file_path is None:
                continue
            if RAW_MODE:
                strm_path = os.path.join(MOUNT_PATH, file_path, f"{file_name}.strm")
            else:
                mount_category = getMountCategory(download.get("metadata_mediatype"))
                if mount_category is None:
                    continue
                strm_path = os.path.join(
                    MOUNT_PATH, mount_category, file_path, f"{file_name}.strm"
                )
            urls = path_to_urls.setdefault(strm_path, [])
            if url not in urls:
                urls.append(url)

        new_strm_files = set(path_to_urls.keys())
        for strm_path, urls in path_to_urls.items():
            writeStremFile(strm_path, urls)

        # Remove .strm files for deleted downloads
        for strm_file in existing_strm_files:
            if strm_file not in new_strm_files:
                try:
                    os.remove(strm_file)
                    logging.debug(f"Removed stale .strm file: {strm_file}")
                    # Remove empty directories
                    dir = os.path.dirname(strm_file)
                    while dir != MOUNT_PATH and not os.listdir(dir):
                        os.rmdir(dir)
                        dir = os.path.dirname(dir)
                except Exception as e:
                    logging.error(f"Error removing .strm file: {e}")

        logging.debug(f"Updated {len(all_downloads)} strm files.")
    finally:
        _runStrm_lock.release()
