from os import getenv
from os.path import exists
from time import localtime, strftime
from pandas import Series, DataFrame, read_csv, concat
from google_authorization import GoogleAuth


class VideoInfoes(GoogleAuth):
    def __init__(self):
        super().__init__()

    def __get_all_video_ids(self, video_ids_file: str, save_flag: bool = True) -> DataFrame:
        video_infoes = read_csv(video_ids_file) if exists(video_ids_file) else DataFrame(columns=["id", "type", "title"])

        # ? get the channel's video infoes
        pageToken = ""
        while pageToken is not None:
            results = self._search_videos(pageToken=pageToken)
            video_infoes = concat(
                [
                    video_infoes,
                    DataFrame(
                        [[video["id"]["videoId"], video["id"]["kind"], video["snippet"]["title"]] for video in results["items"]],
                        columns=["id", "type", "title"],
                    ),
                ],
                ignore_index=True,
            )
            pageToken = results.get("nextPageToken", None)

        if save_flag:
            if exists(video_ids_file):
                before = read_csv(video_ids_file)
                video_infoes = concat([before, video_infoes]).drop_duplicates(subset="id", keep="last")
            video_infoes.to_csv(video_ids_file, index=False, encoding="UTF-8")
        return video_infoes

    def __get_all_playlists_ids(self, playlist_ids_file: str, save_flag: bool = True) -> DataFrame:
        playlist_infoes = read_csv(playlist_ids_file) if exists(playlist_ids_file) else DataFrame(columns=["id", "type", "title"])

        # ? get the channel's video infoes
        pageToken = ""
        while pageToken is not None:
            results = self._search_playlists(pageToken=pageToken)
            playlist_infoes = concat(
                [
                    playlist_infoes,
                    DataFrame(
                        [[playlist["id"], playlist["kind"], playlist["snippet"]["title"]] for playlist in results["items"]],
                        columns=["id", "type", "title"],
                    ),
                ],
                ignore_index=True,
            )
            pageToken = results.get("nextPageToken", None)

        if save_flag:
            if exists(playlist_ids_file):
                before = read_csv(playlist_ids_file)
                playlist_infoes = concat([before, playlist_infoes]).drop_duplicates(subset="id", keep="last")
            playlist_infoes.to_csv(playlist_ids_file, index=False, encoding="UTF-8")
        return playlist_infoes

    def __get_all_video_ids_on_playlists(self, playlist_ids: Series, video_ids_file: str, save_flag: bool = True) -> DataFrame:
        video_infoes = read_csv(video_ids_file) if exists(video_ids_file) else DataFrame(columns=["id", "type", "title"])

        # ? get the channel's video infoes
        for playlist_id in playlist_ids:
            pageToken = ""
            while pageToken is not None:
                results = self._get_all_video_ids_on_playlist(playlist_id=playlist_id, pageToken=pageToken)
                video_infoes = concat(
                    [
                        video_infoes,
                        DataFrame(
                            [[video["contentDetails"]["videoId"], video["kind"], ""] for video in results["items"]],
                            columns=["id", "type", "title"],
                        ),
                    ],
                    ignore_index=True,
                )
                pageToken = results.get("nextPageToken", None)

        if save_flag:
            if exists(video_ids_file):
                before = read_csv(video_ids_file)
                video_infoes = concat([before, video_infoes]).drop_duplicates(subset="id", keep="last")
            video_infoes.to_csv(video_ids_file, index=False, encoding="UTF-8")
        return video_infoes

    def __process_per_channel(self, forHandle: str) -> DataFrame:
        self._channel_id = getenv(f"{forHandle[1:]}_id")
        channel_infoes = self.get_channel_infoes(forHandle) if self._channel_id is None else None

        video_ids_file = f"{forHandle[1:]}_video_ids.csv"
        video_ids = self.__get_all_video_ids(video_ids_file=video_ids_file)
        return video_ids

    def __process_per_playlists(self, forHandle: str) -> DataFrame:
        self._channel_id = getenv(f"{forHandle[1:]}_id")
        channel_infoes = self.get_channel_infoes(forHandle) if self._channel_id is None else None

        playlist_ids_file = f"{forHandle[1:]}_playlist_ids.csv"
        playlist_ids = self.__get_all_playlists_ids(playlist_ids_file=playlist_ids_file)

        video_ids_file = f"{forHandle[1:]}_video_ids.csv"
        video_ids = self.__get_all_video_ids_on_playlists(playlist_ids=playlist_ids.id, video_ids_file=video_ids_file)
        return video_ids

    def __process_for_channels(self, handles: str | list = None, base: DataFrame = None) -> DataFrame:
        if handles is None:
            return base

        if isinstance(handles, str):
            data = self.__process_per_channel(forHandle=handles)
            return concat([base, data])

        for forHandle in handles:
            data = self.__process_per_channel(forHandle=forHandle)
            base = concat([base, data])
        return base

    def __process_for_playlists(self, handles: str | list = None, base: DataFrame = None) -> DataFrame:
        if handles is None:
            return base

        if isinstance(handles, str):
            data = self.__process_per_playlists(forHandle=handles)
            return concat([base, data])

        for forHandle in handles:
            data = self.__process_per_playlists(forHandle=forHandle)
            base = concat([base, data])
        return base

    def add_video_infoes(self, video_ids: Series, views_file: str) -> None:
        date = strftime("%Y/%m/%d", localtime())
        new_views = DataFrame([], columns=["video_id", "title", date])

        for start_index in range(0, len(video_ids), 50):
            video_infoes = self._get_video_infoes(video_id=",".join(video_ids[start_index : start_index + 50]))
            new_views = concat(
                [
                    new_views,
                    DataFrame(
                        [[video["id"], video["snippet"]["title"], video["statistics"]["viewCount"]] for video in video_infoes["items"]],
                        columns=["video_id", "title", date],
                    ),
                ],
                ignore_index=True,
            )

        if exists(views_file):
            before = read_csv(views_file).replace(0, None)
            new_views = before.set_index("video_id").combine_first(new_views.set_index("video_id")).reset_index()

        if new_views.columns[1] != "title":
            titles = new_views.pop("title")
            new_views.insert(1, "title", titles)

        new_views = new_views.fillna(0)
        new_views.drop_duplicates(inplace=True)
        new_views.to_csv(views_file, index=False)

    def process(self, handles: str | list = None, handles_for_playlists: str | list = None, total_name: str = None) -> None:
        video_ids = DataFrame(columns=["id", "type", "title"])
        video_ids = self.__process_for_channels(base=video_ids, handles=handles)
        # print("Process for Channels Finish")
        video_ids = self.__process_for_playlists(base=video_ids, handles=handles_for_playlists)
        # print("Process for Playlists Finish")

        if len(video_ids) > 0:
            if total_name is not None:
                pass
            elif isinstance(handles, str) and handles is not None:
                total_name = handles[1:]
            elif isinstance(handles_for_playlists, str) and handles_for_playlists is not None:
                total_name = handles_for_playlists[1:]
            elif handles is not None:
                total_name = handles[0][1:]
            elif handles_for_playlists is not None:
                total_name = handles_for_playlists[0][1:]

            views_file = f"{total_name}_video_views.csv"
            self.add_video_infoes(video_ids=video_ids.id, views_file=views_file)
        print("Process Finishes!!")


if __name__ == "__main__":
    scrapper = VideoInfoes()
    scrapper.process(handles=["@YouTube"], handles_for_playlists="@GoogleDevelopers")
