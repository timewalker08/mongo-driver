from pymongo.read_preferences import ReadPreference


class SlaveOkSetting(object):
    PRIMARY = 1
    OFFLINE = 2

    TO_PYMONGO = {
        PRIMARY: ReadPreference.PRIMARY,
        OFFLINE: ReadPreference.SECONDARY
    }
