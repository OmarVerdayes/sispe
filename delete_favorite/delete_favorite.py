import os
import logging
import json
from sqlalchemy import create_engine, MetaData, Table, Column, String, BINARY, Integer, Enum, ForeignKey, DATETIME, and_

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import select

#Configuracion del logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#Configuracion de la base de datos
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")
DB_HOST = os.environ.get("DB_HOST")
db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
db_connection = create_engine(db_connection_str)
metadata = MetaData()

#Definicion de la tabla films para agregar atributos foraneos a la tabla favorites
films = Table('film', metadata,
                   Column('film_id', BINARY(16), primary_key=True),
                   Column('title', String(60), nullable=False),
                   Column('description', String(60), nullable=False),
                   Column('length', Integer, nullable=False),
                   Column('status', Enum('Activo', 'Inactivo', name='status_enum'), nullable=False),
                   Column('fk_category', BINARY(16), ForeignKey('categories.category_id'), nullable=False)
              )

#Definicion de la tabla users para agregar los atributos foraneos a la tabla favorites
users = Table('users',metadata,
              Column('user_id', BINARY(16), primary_key=True),
              Column('name', String(60), nullable=False),
              Column('lastname', String(60), nullable=False),
              Column('email', String(100), nullable=False),
              Column('password', String(225), nullable=False),
              Column('fk_rol', BINARY(16), ForeignKey('roles.rol_id'), nullable=False),
              Column('fk_subscription', BINARY(16), ForeignKey('subscription.subscription_id'), nullable=False),)

#Definicion de la tabla favorites
favorites = Table('favorites',metadata,
                  Column('favorite_id',BINARY(16), primary_key=True),
                  Column('fk_user',BINARY(16), ForeignKey('users.user_id'), nullable=False),
                  Column('fk_film',BINARY(16), ForeignKey('films.film_id'), nullable=False),)

def is_hex(s):
    return len(s) == 32 and all(c in '0123456789abcdefABCDEF' for c in s)

#Funcion Lambda para quitar una pelicula de la lista de favoritos
def lambda_handler(event, context):
    try:
        if event.get('body') is None:
            return{
                'statusCode': 400,
                'body': json.dumps('Entrada invalida, cuerpo no encontrado')
            }

        logger.info("deleting favorite")
        data = json.loads(event['body'])

        fk_user = data.get('fk_user')
        fk_film = data.get('fk_film')

        if not fk_user and not fk_film:
            return {
                'statusCode': 400,
                'body': json.dumps('usuario y pelicula necesario')
            }

        if not is_hex(fk_user) or not is_hex(fk_film):
            return {
                'statusCode': 400,
                'body': json.dumps('ID de usuario o película no es válido')
            }

        user_id = bytes.fromhex(fk_user)
        film_id = bytes.fromhex(fk_film)
        conn = db_connection.connect()

        #Verificar si el usuario existe
        user_query = select([users]).where(users.c.user_id == user_id)
        user_result = conn.execute(user_query).fetchone()

        if user_result is None:
            conn.close()
            return {
                'statusCode': 400,
                'body': json.dumps('Usuario no encontrado')
            }

        #Verificar si la pelicula ya esta en favoritos
        query = select([favorites]).where(
            and_(
                favorites.c.fk_user == user_id,
                favorites.c.fk_film == film_id
            )
        )
        result = conn.execute(query)
        existing_favorites = result.fetchone()

        if not existing_favorites:
            conn.close()
            return{
                'statusCode':400,
                'body':json.dumps('Pelicula no esta en la lista de favoritos')
            }

        #Eliminar pelicula de la lista de favoritos
        query = favorites.delete().where(
            and_(
                favorites.c.fk_user == user_id,
                favorites.c.fk_film == film_id,
            )
        )
        conn.execute(query)
        conn.close()

        return{
            'statusCode':200,
            'body':json.dumps('Se ha eliminado la pelicula de la lista de favoritos')
        }
    except SQLAlchemyError as e:
        logger.error(f'Error deleting favorite: {e}')
        return {
            'statusCode':500,
            'body':json.dumps('Error al eliminar la pelicula de la lista de favoritos')
        }
    except json.JSONDecodeError as e:
        logger.error(f'Invalid JSON format: {e}')
        return{
            'statusCode':400,
            'body':json.dumps(f'Formato invalido')
        }