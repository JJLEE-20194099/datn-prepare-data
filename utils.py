class Redis:
    def __init__(self):
        self.redis_host = os.getenv('REDIS_HOST')
        self.redis_port = os.getenv('REDIS_PORT')
        self.redis_password = os.getenv('REDIS_PASSWORD')
        self.redis = redis.Redis(host=self.redis_host, port=self.redis_port, password=self.redis_password, decode_responses=True)


    def check_id_exist(self, id, set_name):
        """_summary_

        Args:
            id (_type_): id of data ( generate by url or id of data in website)
            set_name (_type_): name of set in redis

        Returns:
            _type_: False if id not exist and no add to set, True if id exist
        """
        return self.redis.sismember(set_name, id)

    def add_id_to_set(self, id, set_name):
        """_summary_

        Args:
            id (_type_): id of data ( generate by url or id of data in website)
            set_name (_type_): name of set in redis

        Returns:
            _type_: False if add fail, True if add success
        """
        return self.redis.sadd(set_name, id)

