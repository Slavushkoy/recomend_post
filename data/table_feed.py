from database import Base, SessionLocal
from sqlalchemy import Column, Integer, String, TIMESTAMP, ForeignKey
from sqlalchemy.orm import relationship
from table_post import Post
from table_user import User

class Feed(Base):
    __tablename__ = "feed_action"
    action = Column(String)
    post_id = Column(Integer, ForeignKey(Post.id), primary_key=True)
    post = relationship(Post)
    time = Column(TIMESTAMP)
    user_id = Column(Integer, ForeignKey(User.id), primary_key=True)
    user = relationship(User)

# if __name__ == "__main__":
#     Base.metadata.create_all(engine)

# if __name__ == "__main__":
#     session = SessionLocal()
#     print([post.id for post in (session.query(Post).filter(Post.topic == "business").order_by(Post.id.desc()).limit(10).all())])
#
