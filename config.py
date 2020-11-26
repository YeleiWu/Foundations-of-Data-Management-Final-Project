class BaseConfig:
    SECRET_KEY = 'dsci551'


class DevelopmentConfig(BaseConfig):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = "mysql+pymysql://root:root@localhost:3306/?charset=utf8mb4"
    # SQLALCHEMY_DATABASE_URI = "mysql://root:root@ec2-3-101-118-230.us-west-1.compute.amazonaws.com/airbnb?charset=utf8mb4"


configs = {
    'development': DevelopmentConfig
}
