from json import loads, dumps
from APIs.Google.google_authorizations import GoogleAuth


class GoogleAuth(GoogleAuth):
    def __init__(self):
        SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
        super().__init__(".", "youtube", SCOPES)

    def get_channel_infoes(self, forHandle: str, save_flag: bool = True) -> dict:
        part = "statistics"
        request = self._service.channels().list(part=part, forHandle=forHandle)

        results = loads(dumps(request.execute()))
        channel_infoes = results["items"][0]
        self._channel_id = channel_infoes["id"]

        if save_flag:
            with open(".env", "a") as ef:
                ef.write(f'{forHandle[1:]}_id="{self._channel_id}"\n')
        return channel_infoes

    def _search_videos(self, maxResults: int = 50, pageToken: str = "") -> dict:
        part = "snippet"
        # eventType = "completed"  # live
        order = "date"
        safeSearch = "strict"  # moderate, none, strict
        types = "video"
        videoCaption = "any"
        videoDefinition = "any"
        videoDimension = "any"
        videoDuration = "any"  # long((20, infinite)), medium([4 , 20]), short([0, 4))
        videoEmbeddable = "any"
        videoLicense = "any"  # creativeCommon, youtube
        videoPaidProductPlacement = "any"
        videoSyndicated = "any"
        videoType = "any"  # episode, movie

        request = self._service.search().list(
            part=part,
            channelId=self._channel_id,
            maxResults=maxResults,
            order=order,
            pageToken=pageToken,
            safeSearch=safeSearch,
            type=types,
            videoCaption=videoCaption,
            videoDefinition=videoDefinition,
            videoDimension=videoDimension,
            videoDuration=videoDuration,
            videoEmbeddable=videoEmbeddable,
            videoLicense=videoLicense,
            videoPaidProductPlacement=videoPaidProductPlacement,
            videoSyndicated=videoSyndicated,
            videoType=videoType,
        )
        return loads(dumps(request.execute()))

    def _search_playlists(self, maxResults: int = 50, pageToken: str = "") -> dict:
        request = self._service.playlists().list(part="snippet", channelId=self._channel_id, maxResults=maxResults, pageToken=pageToken)
        return loads(dumps(request.execute()))

    def _get_all_video_ids_on_playlist(self, playlist_id: str, maxResults: int = 50, pageToken: str = "") -> dict:
        request = self._service.playlistItems().list(
            part="contentDetails", maxResults=maxResults, pageToken=pageToken, playlistId=playlist_id
        )
        return loads(dumps(request.execute()))

    def _get_video_infoes(self, video_id: str) -> dict:
        request = self._service.videos().list(part="snippet,statistics", id=video_id)
        return loads(dumps(request.execute()))
