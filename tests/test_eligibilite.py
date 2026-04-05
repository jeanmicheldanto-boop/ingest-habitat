"""
Tests des règles d'éligibilité AVP
"""
import pytest
import sys
from pathlib import Path

# Ajouter le parent au path pour les imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from enrichment.eligibilite_rules import deduce_eligibilite_statut, is_avp_eligible, should_enrich_avp_data


class TestEligibiliteRules:
    """Tests des règles d'éligibilité AVP"""
    
    def test_habitat_inclusif_sans_csv_doit_etre_a_verifier(self):
        """Habitat inclusif sans valeur CSV doit être a_verifier"""
        result = deduce_eligibilite_statut(
            "habitat inclusif", 
            mention_avp_explicite=False,
            eligibilite_csv=None
        )
        assert result == "a_verifier"
    
    def test_habitat_inclusif_avec_mention_avp_doit_etre_a_verifier(self):
        """Habitat inclusif avec mention AVP doit QUAND MÊME être a_verifier"""
        result = deduce_eligibilite_statut(
            "habitat inclusif", 
            mention_avp_explicite=True,
            eligibilite_csv=None
        )
        assert result == "a_verifier"
    
    def test_habitat_inclusif_csv_avp_eligible_doit_etre_preserve(self):
        """Habitat inclusif déjà avp_eligible dans CSV doit être préservé"""
        result = deduce_eligibilite_statut(
            "habitat inclusif", 
            mention_avp_explicite=False,
            eligibilite_csv="avp_eligible"
        )
        assert result == "avp_eligible"
    
    def test_residence_services_seniors_jamais_eligible(self):
        """Résidence services seniors JAMAIS éligible"""
        # Sans mention AVP
        result1 = deduce_eligibilite_statut(
            "résidence services seniors", 
            mention_avp_explicite=False
        )
        assert result1 == "non_eligible"
        
        # Avec mention AVP
        result2 = deduce_eligibilite_statut(
            "résidence services seniors", 
            mention_avp_explicite=True
        )
        assert result2 == "non_eligible"
    
    def test_residence_autonomie_jamais_eligible(self):
        """Résidence autonomie JAMAIS éligible"""
        result = deduce_eligibilite_statut(
            "résidence autonomie", 
            mention_avp_explicite=True
        )
        assert result == "non_eligible"
    
    def test_marpa_jamais_eligible(self):
        """MARPA JAMAIS éligible"""
        result = deduce_eligibilite_statut(
            "MARPA", 
            mention_avp_explicite=True
        )
        assert result == "non_eligible"
    
    def test_beguinage_jamais_eligible(self):
        """Béguinage JAMAIS éligible"""
        result = deduce_eligibilite_statut(
            "béguinage", 
            mention_avp_explicite=True
        )
        assert result == "non_eligible"
    
    def test_accueil_familial_jamais_eligible(self):
        """Accueil familial JAMAIS éligible"""
        result = deduce_eligibilite_statut(
            "accueil familial", 
            mention_avp_explicite=True
        )
        assert result == "non_eligible"
    
    def test_village_seniors_jamais_eligible(self):
        """Village seniors JAMAIS éligible"""
        result = deduce_eligibilite_statut(
            "village seniors", 
            mention_avp_explicite=True
        )
        assert result == "non_eligible"
    
    def test_colocation_sans_mention_non_eligible(self):
        """Colocation sans mention AVP → non_eligible"""
        result = deduce_eligibilite_statut(
            "colocation avec services", 
            mention_avp_explicite=False
        )
        assert result == "non_eligible"
    
    def test_colocation_avec_mention_avp_eligible(self):
        """Colocation avec mention AVP → avp_eligible"""
        result = deduce_eligibilite_statut(
            "colocation avec services", 
            mention_avp_explicite=True
        )
        assert result == "avp_eligible"
    
    def test_habitat_intergenerationnel_avec_mention(self):
        """Habitat intergénérationnel éligible SI mention AVP"""
        result_sans = deduce_eligibilite_statut(
            "habitat intergénérationnel", 
            mention_avp_explicite=False
        )
        assert result_sans == "non_eligible"
        
        result_avec = deduce_eligibilite_statut(
            "habitat intergénérationnel", 
            mention_avp_explicite=True
        )
        assert result_avec == "avp_eligible"
    
    def test_habitat_alternatif_avec_mention(self):
        """Habitat alternatif éligible SI mention AVP"""
        result = deduce_eligibilite_statut(
            "habitat alternatif", 
            mention_avp_explicite=True
        )
        assert result == "avp_eligible"
    
    def test_categorie_inconnue_par_defaut(self):
        """Catégorie inconnue → a_verifier par défaut"""
        result = deduce_eligibilite_statut(
            "catégorie totalement inconnue", 
            mention_avp_explicite=False
        )
        assert result == "a_verifier"
    
    def test_categorie_vide_avec_csv(self):
        """Si catégorie vide mais CSV présent, garder CSV"""
        result = deduce_eligibilite_statut(
            "", 
            mention_avp_explicite=False,
            eligibilite_csv="non_eligible"
        )
        assert result == "non_eligible"


class TestEligibiliteHelpers:
    """Tests des fonctions helper"""
    
    def test_is_avp_eligible_true(self):
        """Test is_avp_eligible avec avp_eligible"""
        assert is_avp_eligible("avp_eligible") == True
    
    def test_is_avp_eligible_false(self):
        """Test is_avp_eligible avec non_eligible"""
        assert is_avp_eligible("non_eligible") == False
        assert is_avp_eligible("a_verifier") == False
    
    def test_should_enrich_avp_data(self):
        """Test should_enrich_avp_data"""
        assert should_enrich_avp_data("avp_eligible") == True
        assert should_enrich_avp_data("non_eligible") == False
        assert should_enrich_avp_data("a_verifier") == False


class TestCasReeels:
    """Tests basés sur les cas réels du CSV"""
    
    def test_cas_habitat_inclusif_le_toit(self):
        """Habitat inclusif 'Le Toit' - avp_eligible dans CSV doit être préservé"""
        result = deduce_eligibilite_statut(
            "habitat inclusif",
            mention_avp_explicite=True,  # Conventionné AVP mentionné
            eligibilite_csv="avp_eligible"
        )
        assert result == "avp_eligible"
    
    def test_cas_beguinage_tarbes(self):
        """Béguinage de Tarbes - a_verifier dans CSV mais doit être non_eligible"""
        result = deduce_eligibilite_statut(
            "béguinage",
            mention_avp_explicite=False,
            eligibilite_csv="a_verifier"
        )
        assert result == "non_eligible"  # Béguinage JAMAIS éligible
    
    def test_cas_colocation_cosima(self):
        """Colocation seniors Cosima - a_verifier doit rester si pas de mention"""
        result = deduce_eligibilite_statut(
            "colocation avec services",
            mention_avp_explicite=False,
            eligibilite_csv="a_verifier"
        )
        assert result == "non_eligible"  # Sans mention AVP → non_eligible
    
    def test_cas_residence_cant_adour(self):
        """Résidence intergénérationnelle - a_verifier si pas de mention AVP"""
        result = deduce_eligibilite_statut(
            "habitat intergénérationnel",
            mention_avp_explicite=False,
            eligibilite_csv="a_verifier"
        )
        # Sans mention AVP explicite → non_eligible
        assert result == "non_eligible"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
