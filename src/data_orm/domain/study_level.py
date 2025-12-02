from sqlalchemy import Column, Integer, String
from .base import Base

class StudyLevel(Base):
    __tablename__ = 'study_level'

    id = Column(Integer, primary_key=True, autoincrement=True)
    label = Column(String(50), nullable=False, unique=True)

    def __init__(self, label: str):
        self.label = label

    def __repr__(self):
        return f"<StudyLevel(id={self.id}, label='{self.label}')>"