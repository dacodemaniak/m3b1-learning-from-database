from sqlalchemy import Column, Integer, String
from .base import Base

class Area(Base):
    __tablename__ = 'area'

    id = Column(Integer, primary_key=True, autoincrement=True)
    label = Column(String(75), nullable=False, unique=True)

    def __init__(self, label: str):
        self.label = label

    def __repr__(self):
        return f"<Area(id={self.id}, label='{self.label}')>"