from typing import Optional
from sqlalchemy import Column, Integer, String, Float, Date, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from datetime import date
from .base import Base

class Person(Base):
    __tablename__ = 'persons'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    lastname = Column(String(100), nullable=False)
    firstname = Column(String(100), nullable=False)
    age = Column(Integer, nullable=False)
    height = Column(Float, nullable=False)
    weight = Column(Float, nullable=False)
    gender = Column(String(1), nullable=False)  # 'H' or 'F'
    sport_licence = Column(Boolean, nullable=False)
    smoker = Column(Boolean, nullable=False)
    french_nationality = Column(Boolean, nullable=False)
    estimated_revenue = Column(Float, nullable=False)
    credit_history = Column(Float, nullable=True)  # Maybe null
    personal_risk = Column(Float, nullable=False)
    account_creation_date = Column(Date, nullable=False)
    credit_score = Column(Float, nullable=True)  # Maybe null
    mensual_home_rent = Column(Float, nullable=True)  # Maybe null
    credit_amount = Column(Float, nullable=False)
    
    child_number = Column(Integer, nullable=True)
    caf_quotient = Column(Float, nullable=True)
    # Foreign keys
    study_level_id = Column(Integer, ForeignKey('study_level.id'), nullable=True)
    area_id = Column(Integer, ForeignKey('area.id'), nullable=True)
    family_situation_id = Column(Integer, ForeignKey('family_situation.id'), nullable=True)
    
    # Relations
    study_level = relationship("StudyLevel", backref="person")
    area = relationship("Area", backref="person")
    family_situation = relationship("FamilySituation", backref="person")
    
    def __init__(
        self,
        lastname: str,
        firstname: str,
        age: Optional[int],
        height: Optional[float],
        weight: Optional[float],
        gender: str,
        sport_licence: bool,
        smoker: bool,
        french_nationality: bool,
        estimated_revenue: Optional[float],
        personal_risk: Optional[float],
        account_creation_date: Optional[date],
        credit_amount: Optional[float],
        credit_history: Optional[float] = None,
        credit_score: Optional[float] = None,
        mensual_home_rent: Optional[float] = None,
        study_level_id: Optional[int] = None,
        area_id: Optional[int] = None,
        family_situation_id: Optional[int] = None
    ):
        self.lastname = lastname
        self.firstname = firstname
        self.age = age
        self.height = height
        self.weight = weight
        self.gender = gender.upper() if gender else 'H'
        self.sport_licence = sport_licence
        self.smoker = smoker
        self.french_nationality = french_nationality
        self.estimated_revenue = estimated_revenue
        self.credit_history = credit_history
        self.personal_risk = personal_risk
        self.account_creation_date = account_creation_date
        self.credit_score = credit_score
        self.mensual_home_rent = mensual_home_rent
        self.credit_amount = credit_amount
        self.study_level_id = study_level_id
        self.area_id = area_id
        self.family_situation_id = family_situation_id
        

        def to_dict(self) -> dict:
            """Data conversion for API"""
            return {
                'id': self.id,
                'lastname': self.lastname,
                'firstname': self.firstname,
                'age': self.age,
                'height': self.height,
                'weight': self.weight,
                'gender': self.gender,
                'sport_licence': self.sport_licence,
                'smoker': self.smoker,
                'french_nationality': self.french_nationality,
                'estimated_revenue': self.estimated_revenue,
                'credit_history': self.credit_history,
                'personal_risk': self.personal_risk,
                'account_creation_date': self.account_creation_date.isoformat() if self.account_creation_date else None,
                'credit_score': self.credit_score,
                'mensual_home_rent': self.mensual_home_rent,
                'credit_amount': self.credit_amount,
                'study_level': self.study_level.label if self.study_level else None,
                'area': self.area.label if self.area else None,
                'family_situation': self.family_situation.label if self.family_situation else None
            }

    def __repr__(self):
        return f"<Person(id={self.id}, name='{self.firstname} {self.lastname}', age={self.age})>"