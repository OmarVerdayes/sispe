import os
import logging
import json
import uuid
from sqlalchemy import create_engine, MetaData, Table, Column, String, BINARY, Integer, Enum, ForeignKey, and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import select

# Configuración del logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuración de la base de datos
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")
DB_HOST = os.environ.get("DB_HOST")
db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
db_connection = create_engine(db_connection_str)
metadata = MetaData()

# Definición de la tabla films
films = Table('films', metadata,
              Column('film_id', BINARY(16), primary_key=True),
              Column('title', String(60), nullable=False),
              Column('description', String(60), nullable=False),
              Column('length', Integer, nullable=False),
              Column('status', Enum('activo', 'inactivo', name='status_enum'), nullable=False),
              Column('fk_category', BINARY(16), ForeignKey('categories.category_id'), nullable=False))

# Definición de la tabla users
users = Table('users', metadata,
              Column('user_id', BINARY(16), primary_key=True),
              Column('name', String(60), nullable=False),
              Column('lastname', String(60), nullable=False),
              Column('email', String(100), nullable=False),
              Column('password', String(225), nullable=False),
              Column('fk_rol', BINARY(16), ForeignKey('roles.rol_id'), nullable=False),
              Column('fk_subscription', BINARY(16), ForeignKey('subscription.subscription_id'), nullable=False))

# Definición de la tabla favorites
favorites = Table('favorites', metadata,
                  Column('favorite_id', BINARY(16), primary_key=True),
                  Column('fk_user', BINARY(16), ForeignKey('users.user_id'), nullable=False),
                  Column('fk_film', BINARY(16), ForeignKey('films.film_id'), nullable=False))

def is_hex(s):
    return len(s) == 32 and all(c in '0123456789abcdefABCDEF' for c in s)

def lambda_handler(event, context):
    try:
        if event.get('body') is None:
            return {
                'statusCode': 400,
                'body': json.dumps('Entrada invalida, cuerpo no encontrado')
            }

        logger.info("Creating favorite")
        data = json.loads(event['body'])
        fk_user = data.get('fk_user')
        fk_film = data.get('fk_film')

        if not fk_user or not fk_film:
            return {
                'statusCode': 400,
                'body': json.dumps('Entrada invalida, usuario o pelicula no encontrados')
            }

        if not is_hex(fk_user) or not is_hex(fk_film):
            return {
                'statusCode': 400,
                'body': json.dumps('El ID de usuario o pelicula no es válido')
            }

        user_id = bytes.fromhex(fk_user)
        film_id = bytes.fromhex(fk_film)

        conn = db_connection.connect()

        # Verificar si el usuario existe
        user_query = select([users]).where(users.c.user_id == user_id)
        user_result = conn.execute(user_query).fetchone()
        if user_result is None:
            conn.close()
            return {
                'statusCode': 400,
                'body': json.dumps('Usuario no encontrado')
            }

        # Verificar si la película existe y está activa
        film_query = select([films]).where(
            and_(
                films.c.film_id == film_id,
                films.c.status == 'activo'
            )
        )
        film_result = conn.execute(film_query).fetchone()
        if film_result is None:
            conn.close()
            return {
                'statusCode': 400,
                'body': json.dumps('Película no encontrada o no está activa')
            }

        # Verificar si la película ya está en favoritos
        favorite_query = select([favorites]).where(
            and_(
                favorites.c.fk_user == user_id,
                favorites.c.fk_film == film_id
            )
        )
        existing_favorites = conn.execute(favorite_query).fetchone()
        if existing_favorites:
            conn.close()
            return {
                'statusCode': 400,
                'body': json.dumps('Película ya agregada a la lista de favoritos')
            }

        # Insertar la película a favoritos
        favorite_id = uuid.uuid4().bytes
        insert_query = favorites.insert().values(
            favorite_id=favorite_id,
            fk_user=user_id,
            fk_film=film_id
        )
        conn.execute(insert_query)
        conn.close()

        return {
            'statusCode': 200,
            'body': json.dumps('Película agregada a la lista de favoritos')
        }

    except SQLAlchemyError as e:
        logger.error(f'Error adding favorite: {e}')
        return {
            'statusCode': 500,
            'body': json.dumps('Error al agregar a favoritos')
        }
    except json.JSONDecodeError as e:
        logger.error(f'Invalid JSON format: {e}')
        return {
            'statusCode': 400,
            'body': json.dumps('Formato JSON inválido')
        }