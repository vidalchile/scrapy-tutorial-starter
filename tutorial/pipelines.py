import logging

from sqlalchemy.orm import sessionmaker
from scrapy.exceptions import DropItem
from tutorial.models import Quote, Author, Tag, db_connect, create_table


class DuplicatesPipeline(object):
    
    def __init__(self):
        """
        Initializes database connection and sessionmaker.
        Creates tables.
        """
        engine = db_connect()
        create_table(engine)
        self.Session = sessionmaker(bind=engine)
        logging.info("****DuplicatesPipeline: database connected****")

    def process_item(self, item, spider):
        session = self.Session()
        exist_quote = session.query(Quote).filter_by(quote_content = item["quote_content"]).first()
        if exist_quote is not None:  # the current quote exists
            raise DropItem("Duplicate item found: %s" % item["quote_content"])
            session.close()
        else:
            return item
            session.close()



class SaveQuotesPipeline(object):
    def __init__(self):
        """
        Initializes database connection and sessionmaker
        Creates tables
        La función init a continuación usa las funciones de models.py
        para conectarse a la base de datos ( db_connect) y crear tablas ( create_table) 
        si aún no existían (de lo contrario, se ignoran).
        """
        engine = db_connect()
        create_table(engine)
        self.Session = sessionmaker(bind=engine)


    def process_item(self, item, spider):
        """
        Primero creo instancias para la sesión de base de datos y tres tablas. 
        Luego, asigno la información del autor y cito valores de texto a las columnas 
        de la tabla correspondientes.
        Save quotes in the database
        This method is called for every item pipeline component
        """
        session = self.Session()
        quote = Quote()
        author = Author()
        tag = Tag()
        author.name = item["author_name"]
        author.birthday = item["author_birthday"]
        author.bornlocation = item["author_bornlocation"]
        author.bio = item["author_bio"]
        quote.quote_content = item["quote_content"]

        # check whether the author exists
        exist_author = session.query(Author).filter_by(name = author.name).first()
        if exist_author is not None:  # the current author exists
            quote.author = exist_author
        else:
            quote.author = author

        # check whether the current quote has tags or not
        if "tags" in item:
            for tag_name in item["tags"]:
                tag = Tag(name=tag_name)
                # check whether the current tag already exists in the database
                exist_tag = session.query(Tag).filter_by(name = tag.name).first()
                if exist_tag is not None:  # the current tag exists
                    tag = exist_tag
                quote.tags.append(tag)

        try:
            session.add(quote)
            session.commit()

        except:
            session.rollback()
            raise

        finally:
            session.close()

        return item