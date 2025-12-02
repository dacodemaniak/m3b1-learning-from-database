from sqlalchemy import Column, Integer, String
from .base import Base

class FamilySituation(Base):
    __tablename__ = 'family_situation'

    id = Column(Integer, primary_key=True, autoincrement=True)
    label = Column(String(50), nullable=False, unique=True)

    def __init__(self, label: str):
        self.label = label

    def __repr__(self):
        return f"<FamilySituation(id={self.id}, label='{self.label}')>"