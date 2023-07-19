# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

class SkyscrapPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        # Strip all whitespaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'description':
                value=adapter.get(field_name)
                adapter[field_name] = value[0].strip()

        # Category & product type -> switch to lowercase
        lowercase_keys = ['category', 'product_type']
        for key in lowercase_keys:
            value=adapter.get(key)
            adapter[key] = value.lower()

        # Price -> convert to float
        

        # Availability -> extract number of books in stock
        availability_string = adapter.get('availability')
        split_string = availability_string.split('(')
        if len(split_string) < 2:
            adapter['availability'] = 0
        else:
            availability = split_string[1].split(' ')
            adapter['availability'] = int(availability[0])
        #Reviews --> convert string to number
        num=adapter.get('number_of_reviews')
        adapter['number_of_reviews']=int(num)
        #Stars --> convert string to number
        stars_string=adapter.get('stars')
        split_string_stars = stars_string.split(' ')
        stars_text_value=split_string_stars[1].lower()
        if stars_text_value=="zero":
            adapter['stars']=0

        elif stars_text_value=="one":   
            adapter['stars']=1

        elif stars_text_value=="two":
            adapter['stars']=2
        elif stars_text_value == "three" :
            adapter['stars']=3
        elif stars_text_value == "four" :
            adapter['stars']=4
        elif stars_text_value == "five" :
            adapter['stars']=5

        return item
import mysql.connector
class SaveToMySQLPipeline:
    def __init__(self) :
        self.conn=mysql.connector.connect(
             host = "localhost",
                user = "root",
                password = "3031970",
                database='books'
        )
        ##create cursor,used to execute command
        self.curr=self.conn.cursor()
        self.curr.execute(
            """CREATE TABLE IF NOT EXISTS books(
                id int NOT NULL auto_increment,
                url VARCHAR(255),
                title text,
                upc VARCHAR(255),
                product_type VARCHAR(255),
                price_excl_tax VARCHAR(255),
                price_incl_tax VARCHAR(255),
                tax VARCHAR(255),
                availability INTEGER,
                number_of_reviews INTEGER,
                stars VARCHAR(255),
                category VARCHAR(255),
                description VARCHAR(255),
                price VARCHAR(255),

                PRIMARY KEY(id)
            )"""

        )
    def process_item(self,item,spider):
        self.curr.execute("""
            insert into books(
                          url,
                          title,upc,product_type,price_excl_tax,price_incl_tax,tax,availability,
                          number_of_reviews,stars,category,description,price)
                           values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s),

                        """),(
                            item['url'],
                            item['title'],
                            item['upc'],
                            item['product_type'],
                            item['price_excl_tax'],
                            item['price_incl_tax'],
                            item['tax'],
                            item['availability'],
                            item['number_of_reviews'],
                            item['stars'],
                            item['category'],
                           str( item['description'][0]),
                            item['price']

                        )
        self.conn.commit()
        return item
    
    def close_spider(self,spider):

        self.curr.close()

        self.conn.close()
