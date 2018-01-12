"""Models and database functions for Ratings project."""
from flask_sqlalchemy import SQLAlchemy
import correlation
from collections import defaultdict

# This is the connection to the PostgreSQL database; we're getting this through
# the Flask-SQLAlchemy helper library. On this, we can find the `session`
# object, where we do most of our interactions (like committing, etc.)

db = SQLAlchemy()


##############################################################################
# Model definitions

class User(db.Model):
    """User of ratings website."""

    __tablename__ = "users"

    user_id = db.Column(db.Integer, autoincrement=True, primary_key=True)
    email = db.Column(db.String(64), nullable=True)
    password = db.Column(db.String(64), nullable=True)
    age = db.Column(db.Integer, nullable=True)
    zipcode = db.Column(db.String(15), nullable=True)

    def __repr__(self):
        """Provide helpful representation when printed."""

        return "<User user_id=%s email=%s>" % (self.user_id, self.email)

    def predict_rating(self, movie):
        """Predict user's rating of a movie."""

        #
        # option 1: SQLAlchemy ORM
        #
        UserMovies = db.aliased(Rating)
        MovieUsers = db.aliased(Rating)

        query = (db.session.query(Rating.user_id, Rating.score, UserMovies.score, MovieUsers.score)
                 .join(UserMovies, UserMovies.movie_id == Rating.movie_id)
                 .join(MovieUsers, Rating.user_id == MovieUsers.user_id)
                 .filter(UserMovies.user_id == self.user_id)
                 .filter(MovieUsers.movie_id == movie.movie_id))

        #
        # option 2: raw SQL
        #
        # sql = """
        #     SELECT ratings.user_id, ratings.score, user_movies.score, movie_users.score
        #     FROM ratings AS user_movies
        #       JOIN ratings
        #         ON (user_movies.movie_id = ratings.movie_id)
        #       JOIN ratings AS movie_users
        #         ON (ratings.user_id = movie_users.user_id)
        #     WHERE user_movies.user_id = :user_id
        #       AND movie_users.movie_id = :movie_id
        #     """
        #
        # query = db.session.execute(sql, dict(user_id=self.user_id, movie_id=movie.movie_id))
        #

        known_ratings = {}
        paired_ratings = defaultdict(list)

        for rating_user_id, rating_score, user_movie_score, movie_user_score in query:
            paired_ratings[rating_user_id].append((user_movie_score, rating_score))
            known_ratings[rating_user_id] = movie_user_score

        similarities = []

        for _id, score in known_ratings.iteritems():
            similarity = correlation.pearson(paired_ratings[_id])
            if similarity > 0:
                similarities.append((similarity, score))

        if not similarities:
            return None

        numerator = sum([score * sim for sim, score in similarities])
        denominator = sum([sim for sim, score in similarities])

        return numerator / denominator


class Movie(db.Model):
    """Movie on ratings website."""

    __tablename__ = "movies"

    movie_id = db.Column(db.Integer, autoincrement=True, primary_key=True)
    title = db.Column(db.String(100))
    released_at = db.Column(db.DateTime)
    imdb_url = db.Column(db.String(200))

    def __repr__(self):
        """Provide helpful representation when printed."""

        return "<Movie movie_id=%s title=%s>" % (self.movie_id, self.title)


class Rating(db.Model):
    """Rating of a movie by a user."""

    __tablename__ = "ratings"

    rating_id = db.Column(db.Integer, autoincrement=True, primary_key=True)
    movie_id = db.Column(db.Integer, db.ForeignKey('movies.movie_id'), index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.user_id'), index=True)
    score = db.Column(db.Integer)

    # Define relationship to user
    user = db.relationship("User",
                           backref=db.backref("ratings", order_by=rating_id))

    # Define relationship to movie
    movie = db.relationship("Movie",
                            backref=db.backref("ratings", order_by=rating_id))

    def __repr__(self):
        """Provide helpful representation when printed."""

        return "<Rating rating_id=%s movie_id=%s user_id=%s score=%s>" % (
            self.rating_id, self.movie_id, self.user_id, self.score)


##############################################################################
# Helper functions

def connect_to_db(app):
    """Connect the database to our Flask app."""

    # Configure to use our PostgreSQL database
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql:///ratings'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ECHO'] = True
    db.app = app
    db.init_app(app)


if __name__ == "__main__":
    # As a convenience, if we run this module interactively, it will leave
    # you in a state of being able to work with the database directly.

    from server import app

    connect_to_db(app)
    print "Connected to DB."
