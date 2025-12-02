from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import date

# --- Modèles de base pour la création/mise à jour ---

class PersonBase(BaseModel):
    """Schéma de base pour la création d'une Personne."""
    # Champs obligatoires (nullable=False dans SQLAlchemy)
    lastname: str = Field(..., max_length=100)
    firstname: str = Field(..., max_length=100)
    age: int = Field(..., ge=0)
    height: float = Field(..., gt=0)
    weight: float = Field(..., gt=0)
    gender: str = Field(..., pattern=r'^[HF]$', max_length=1, description="Doit être 'H' ou 'F'.")
    sport_licence: bool
    smoker: bool
    french_nationality: bool
    estimated_revenue: float = Field(..., ge=0)
    personal_risk: float = Field(..., ge=0)
    account_creation_date: date
    credit_amount: float = Field(..., gt=0)

    # Clés étrangères (nullable=True dans SQLAlchemy)
    study_level_id: Optional[int] = None
    area_id: Optional[int] = None
    family_situation_id: Optional[int] = None

    # Champs optionnels (nullable=True dans SQLAlchemy)
    credit_history: Optional[float] = None
    credit_score: Optional[float] = None
    mensual_home_rent: Optional[float] = None

    # --- Validator pour s'assurer que le genre est en majuscule ---
    @field_validator('gender', mode='before')
    def upper_gender(cls, value):
        if isinstance(value, str):
            return value.upper()
        return value

# --- Modèle de réponse (pour inclure l'ID de la DB) ---

class PersonResponse(PersonBase):
    """Schéma de réponse complet pour une Personne (inclut l'ID)."""
    id: int
    
    # Facultatif: Si vous voulez inclure les libellés de relation au lieu des IDs
    # Vous pouvez les ajouter ici et les gérer dans le service ou le contrôleur.
    # study_level_label: Optional[str] = None
    # area_label: Optional[str] = None
    # family_situation_label: Optional[str] = None

    class Config:
        # Permet à Pydantic de lire des données directement de l'objet SQLAlchemy (Person)
        from_attributes = True

# --- Modèle de mise à jour (autorise les champs optionnels) ---

class PersonUpdate(BaseModel):
    """Schéma pour la mise à jour partielle d'une Personne."""
    
    # Tous les champs sont facultatifs (Optional[T] = None)
    lastname: Optional[str] = Field(None, max_length=100)
    firstname: Optional[str] = Field(None, max_length=100)
    age: Optional[int] = Field(None, ge=0)
    height: Optional[float] = Field(None, gt=0)
    weight: Optional[float] = Field(None, gt=0)
    gender: Optional[str] = Field(None, pattern=r'^[HF]$', max_length=1)
    sport_licence: Optional[bool] = None
    smoker: Optional[bool] = None
    french_nationality: Optional[bool] = None
    estimated_revenue: Optional[float] = Field(None, ge=0)
    personal_risk: Optional[float] = Field(None, ge=0)
    account_creation_date: Optional[date] = None
    credit_amount: Optional[float] = Field(None, gt=0)

    study_level_id: Optional[int] = None
    area_id: Optional[int] = None
    family_situation_id: Optional[int] = None

    credit_history: Optional[float] = None
    credit_score: Optional[float] = None
    mensual_home_rent: Optional[float] = None
    
    # Validator pour s'assurer que le genre est en majuscule lors de la mise à jour
    @field_validator('gender', mode='before')
    def upper_gender(cls, value):
        if isinstance(value, str):
            return value.upper()
        return value