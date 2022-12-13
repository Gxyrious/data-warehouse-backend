# coding: utf-8
from flask_sqlalchemy import SQLAlchemy

# flask-sqlacodegen --flask 'mysql+pymysql://liuchang:LiUChAnG200203/n@81.68.102.171/dw' --outfile "movie.py"
db = SQLAlchemy()



class Act(db.Model):
    __tablename__ = 'Act'

    movie_id = db.Column(db.ForeignKey('Movie.movie_id'), primary_key=True, nullable=False)
    actor_id = db.Column(db.ForeignKey('Actor.actor_id'), primary_key=True, nullable=False, index=True)
    movie_title = db.Column(db.String(256), nullable=False)

    actor = db.relationship('Actor', primaryjoin='Act.actor_id == Actor.actor_id', backref='acts')
    movie = db.relationship('Movie', primaryjoin='Act.movie_id == Movie.movie_id', backref='acts')



class Actor(db.Model):
    __tablename__ = 'Actor'

    actor_id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(256))



class Asin(db.Model):
    __tablename__ = 'Asin'

    asin = db.Column(db.String(10), primary_key=True)
    movie_id = db.Column(db.ForeignKey('Movie.movie_id'), nullable=False, index=True)

    movie = db.relationship('Movie', primaryjoin='Asin.movie_id == Movie.movie_id', backref='asins')



class Cooperation(db.Model):
    __tablename__ = 'Cooperation'

    left_person_id = db.Column(db.Integer, primary_key=True, nullable=False)
    right_person_id = db.Column(db.Integer, primary_key=True, nullable=False)
    movie_id = db.Column(db.Integer, primary_key=True, nullable=False)
    type = db.Column(db.Integer, primary_key=True, nullable=False)



class Direct(db.Model):
    __tablename__ = 'Direct'

    movie_id = db.Column(db.ForeignKey('Movie.movie_id'), primary_key=True, nullable=False)
    director_id = db.Column(db.ForeignKey('Director.director_id'), primary_key=True, nullable=False, index=True)
    movie_title = db.Column(db.String(256), nullable=False)

    director = db.relationship('Director', primaryjoin='Direct.director_id == Director.director_id', backref='directs')
    movie = db.relationship('Movie', primaryjoin='Direct.movie_id == Movie.movie_id', backref='directs')



class Director(db.Model):
    __tablename__ = 'Director'

    director_id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(256))



class Format(db.Model):
    __tablename__ = 'Format'

    format_id = db.Column(db.Integer, primary_key=True)
    format_name = db.Column(db.String(256), nullable=False)
    movie_id = db.Column(db.ForeignKey('Movie.movie_id'), nullable=False, index=True)
    movie_title = db.Column(db.String(256), nullable=False)

    movie = db.relationship('Movie', primaryjoin='Format.movie_id == Movie.movie_id', backref='formats')



class Genre(db.Model):
    __tablename__ = 'Genre'

    genre_id = db.Column(db.Integer, primary_key=True)
    genre_name = db.Column(db.String(64), nullable=False)
    movie_id = db.Column(db.ForeignKey('Movie.movie_id'), nullable=False, index=True)
    movie_title = db.Column(db.String(256), nullable=False)

    movie = db.relationship('Movie', primaryjoin='Genre.movie_id == Movie.movie_id', backref='genres')



class Movie(db.Model):
    __tablename__ = 'Movie'

    movie_id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(256))
    score = db.Column(db.Float, nullable=False)
    edition = db.Column(db.Integer, nullable=False)



class ReleaseDate(db.Model):
    __tablename__ = 'ReleaseDate'

    time_id = db.Column(db.Integer, primary_key=True)
    movie_id = db.Column(db.ForeignKey('Movie.movie_id'), nullable=False, index=True)
    year = db.Column(db.Integer)
    month = db.Column(db.Integer)
    day = db.Column(db.Integer)
    season = db.Column(db.Integer)
    weekday = db.Column(db.Integer, info='1-Mon,7-Sun')
    date = db.Column(db.DateTime)

    movie = db.relationship('Movie', primaryjoin='ReleaseDate.movie_id == Movie.movie_id', backref='release_dates')



class Review(db.Model):
    __tablename__ = 'Review'

    movie_id = db.Column(db.ForeignKey('Movie.movie_id'), index=True)
    review_id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(256))
    helpfulness = db.Column(db.String(10))
    review_score = db.Column(db.Float(asdecimal=True))
    review_time = db.Column(db.DateTime)
    review_summary = db.Column(db.String(1024))
    review_text = db.Column(db.String)
    asin = db.Column(db.String(10))

    movie = db.relationship('Movie', primaryjoin='Review.movie_id == Movie.movie_id', backref='reviews')