import os

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': os.getenv('PG_NAME', 'linked_db'),
        'USER': os.getenv('PG_USER', 'postgres'),
        'PASSWORD': os.getenv('PG_PASSWORD', 'secret'),
        'HOST': os.getenv('PG_HOST', 'localhost'),
        'PORT': os.getenv('PG_PORT', '5432'),
    },
    'one_c_raw': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': os.getenv('ONE_C_RAW_NAME', '1C_8_RAW'),
        'USER': os.getenv('ONE_C_RAW_USER', 'ones'),
        'PASSWORD': os.getenv('ONE_C_RAW_PASSWORD', 'ones'),
        'HOST': os.getenv('ONE_C_RAW_HOST', 'localhost'),
        'PORT': os.getenv('ONE_C_RAW_PORT', '3306'),
    },
    'backup': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': os.getenv('BCK_NAME', 'linked_backup'),
        'USER': os.getenv('BCK_USER', 'postgres'),
        'PASSWORD': os.getenv('BCK_PASSWORD', 'secret'),
        'HOST': os.getenv('BCK_HOST', 'localhost'),
        'PORT': os.getenv('BCK_PORT', '5436'),
    }
}

DATABASE_ROUTERS = [
    'conf.routers.DefaultRouter',
    'one_c_raw.router.Router',
]
