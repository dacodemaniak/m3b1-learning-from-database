from sqlalchemy.orm import Session
from data_orm.domain.person import Person
from data_orm.api.persons.models.models import PersonBase, PersonUpdate
from fastapi import HTTPException
from typing import List

class PersonService:
    """Service pour la gestion des entités Personne."""

    def __init__(self, db: Session):
        self.db = db

    def get_all(self, skip: int = 0, limit: int = 100) -> List[Person]:
        """Récupère toutes les personnes (avec pagination optionnelle)."""
        return self.db.query(Person).offset(skip).limit(limit).all()

    def get_by_id(self, person_id: int) -> Person:
        """Récupère une personne par son ID."""
        person = self.db.query(Person).filter(Person.id == person_id).first()
        if not person:
            raise HTTPException(status_code=404, detail=f"Person with id {person_id} not found")
        return person

    def create(self, person_data: PersonBase) -> Person:
        """Crée une nouvelle personne."""
        # Note: Dans un scénario réel, il faudrait vérifier l'existence des IDs des clés étrangères (study_level_id, area_id, etc.)
        
        # Le modèle PersonBase a déjà validé les données
        person_dict = person_data.model_dump()
        
        new_person = Person(**person_dict)
        
        self.db.add(new_person)
        self.db.commit()
        self.db.refresh(new_person)
        return new_person

    def update(self, person_id: int, person_data: PersonUpdate) -> Person:
        """Met à jour une personne existante."""
        person = self.get_by_id(person_id) # Vérifie l'existence et lève 404 si nécessaire
        
        # Obtient uniquement les champs qui ont été fournis dans la requête (exclude_unset=True)
        update_data = person_data.model_dump(exclude_unset=True)
        
        for key, value in update_data.items():
            # Conversion spéciale pour le genre (que nous avions gérée avec un validator Pydantic)
            if key == 'gender' and value is not None:
                 setattr(person, key, value.upper())
            else:
                 setattr(person, key, value)
            
        self.db.commit()
        self.db.refresh(person)
        return person

    def delete(self, person_id: int):
        """Supprime une personne par son ID."""
        person = self.get_by_id(person_id) # Vérifie l'existence et lève 404 si nécessaire
        self.db.delete(person)
        self.db.commit()