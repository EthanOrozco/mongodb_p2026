#!/usr/bin/env python3
import falcon
from bson.objectid import ObjectId


class BookResource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, book_id):
        """Handles GET requests to retrieve a single book"""
        book = self.db.books.find_one({'_id': ObjectId(book_id)})
        if book:
            book['_id'] = str(book['_id'])
            resp.media = book
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_put(self, req, resp, book_id):
        """Handles PUT requests to update a single book"""
        book = self.db.books.find_one({'_id': ObjectId(book_id)})
        if not book:
            resp.status = falcon.HTTP_404
            return
    
        data = await req.media
        data = validate_update_data(data)
        self.db.books.update_one({'_id': ObjectId(book_id)}, {'$set': data})
        book = self.db.books.find_one({'_id': ObjectId(book_id)})
        book['_id'] = str(book['_id'])
        resp.media = book
        resp.status = falcon.HTTP_200  

    async def on_delete(self, req, resp, book_id):
        """Handles DELETE requests to delete a single book"""
        book = self.db.books.find_one({'_id': ObjectId(book_id)})
        if not book:
            resp.status = falcon.HTTP_404
            return

        self.db.books.delete_one({'_id': ObjectId(book_id)})
        resp.status = falcon.HTTP_204


class BooksResource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp):
        """Handles GET requests to retrieve all books"""
        rating = req.get_param_as_float('rating')
        query = {}
        if rating is not None:
            query['average_rating'] = {'$gte': rating}

        books = self.db.books.find(query)
        books_list = []
        for book in books:
            book['_id'] = str(book['_id'])
            books_list.append(book)
        resp.media = books_list
        resp.status = falcon.HTTP_200

    async def on_post(self, req, resp):
        """Handles POST requests to add a new book"""
        data = await req.media
        data = validate_book_data(data)
        result = self.db.books.insert_one(data)
        data['_id'] = str(result.inserted_id)
        resp.media = data
        resp.status = falcon.HTTP_201

book_types = {
    "title": str,
    "authors": list,
    "average_rating": float,
    "isbn": str,
    "isbn13": str,
    "language_code": str,
    "num_pages": int,
    "ratings_count": int,
    "text_reviews_count": int,
    "publication_date": str,
    "publisher":  str,
}

def validate_book_data(data):
    for property in book_types:
        if property not in data: 
            raise falcon.HTTPBadRequest(f"Invalid data: {property} is required.")
        if book_types[property] != str:
            try:
                data[property] = book_types[property](data[property])
            except ValueError:
                raise falcon.HTTPBadRequest(f"Invalid data: {property} must be {book_types[property]}.")
    return data

def validate_update_data(data):
    for property in data:
        if property not in book_types:
            raise falcon.HTTPBadRequest(f"Invalid data: {property} is not a valid book property.")
        if book_types[property] != str:
            try:
                data[property] = book_types[property](data[property])
            except ValueError:
                raise falcon.HTTPBadRequest(f"Invalid data: {property} must be {book_types[property]}.")
    return data