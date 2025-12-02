from fastapi import APIRouter, Depends, status, Query
from sqlalchemy.orm import Session
from data_orm.api.persons.models.models import PersonBase, PersonResponse, PersonUpdate
from data_orm.api.persons.services.person import PersonService
from data_orm.api.infrastructure.dependencies import get_db
from typing import List

router = APIRouter(
    prefix="/persons",
    tags=["Persons"], # Utilisé pour le regroupement dans la documentation Swagger
)

# --- READ ALL (findAll) ---
@router.get("/", response_model=List[PersonResponse])
def find_all_persons(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    """Récupère la liste de toutes les personnes avec pagination."""
    return PersonService(db).get_all(skip=skip, limit=limit)

# --- READ ONE (findOne) ---
@router.get("/{person_id}", response_model=PersonResponse)
def find_one_person(person_id: int, db: Session = Depends(get_db)):
    """Récupère une personne par son ID."""
    return PersonService(db).get_by_id(person_id)

# --- CREATE (add) ---
@router.post("/", response_model=PersonResponse, status_code=status.HTTP_201_CREATED)
def add_person(person_data: PersonBase, db: Session = Depends(get_db)):
    """Crée une nouvelle personne."""
    return PersonService(db).create(person_data)

# --- UPDATE (update) ---
# Note: On utilise le PUT ici pour une mise à jour complète ou PATCH pour une mise à jour partielle.
# La méthode ci-dessous gère la mise à jour partielle grâce au modèle PersonUpdate.
@router.put("/{person_id}", response_model=PersonResponse)
def update_person(person_id: int, person_data: PersonUpdate, db: Session = Depends(get_db)):
    """Met à jour une personne existante (mise à jour partielle via le modèle PersonUpdate)."""
    return PersonService(db).update(person_id, person_data)

# --- DELETE (remove) ---
@router.delete("/{person_id}", status_code=status.HTTP_204_NO_CONTENT)
def remove_person(person_id: int, db: Session = Depends(get_db)):
    """Supprime une personne par son ID."""
    PersonService(db).delete(person_id)
    # FastAPI retourne automatiquement 204 No Content pour une fonction qui ne retourne rien
    return