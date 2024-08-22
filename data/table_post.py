from database import Base, SessionLocal
from sqlalchemy import Column, Integer, String

class Post(Base):
    __tablename__ = "post_text_df"
    id = Column(Integer, primary_key=True)
    text = Column(String)
    topic = Column(String)

# if __name__ == "__main__":
#     Base.metadata.create_all(engine)

if __name__ == "__main__":
    session = SessionLocal()
    print([post.id for post in (session.query(Post).filter(Post.topic == "business").order_by(Post.id.desc()).limit(10).all())])

