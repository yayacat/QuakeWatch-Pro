from datetime import datetime, timezone, timedelta

class Utils:
    def __init__(self):
        self.TZ_UTC_8 = timezone(timedelta(hours=8))

    def get_timestamp_utc8(self,):
        """獲取 UTC+8 時間戳(毫秒)"""
        epoch = datetime(1970, 1, 1, tzinfo=self.TZ_UTC_8)
        return int((self.get_now_utc8() - epoch).total_seconds() * 1000)

    def get_now_utc8(self,):
        """獲取當前 UTC+8 時間"""
        return datetime.now(self.TZ_UTC_8)

    def get_timezone_utc8(self,):
        """獲取 UTC+8 時區"""
        return self.TZ_UTC_8