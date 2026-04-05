"""
Script pour analyser la structure HTML des résidences services seniors
"""
import requests
from bs4 import BeautifulSoup
import re

def main():
    # Configuration
    api_key = '6TLSX0LV0HPNAT3E1D1D0CZUIEPTPV312KEJYFPDI05JPOU71WY6S5NGBIGKA6T7U9VD9MVERLZLI5UY'
    url = 'https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-service/lot-et-garonne-47'

    print("🔍 Récupération contenu avec ScrapingBee...")
    response = requests.get(
        'https://app.scrapingbee.com/api/v1/',
        params={
            'api_key': api_key,
            'url': url,
            'render_js': 'true',
            'wait': '5000'
        }
    )

    print(f"Status: {response.status_code}")
    print(f"Content length: {len(response.text)}")

    soup = BeautifulSoup(response.text, 'html.parser')

    print("\n=== ANALYSE STRUCTURE HTML ===")

    # Chercher les éléments contenant 'Résidence Services'
    residences_elements = soup.find_all(string=re.compile(r'Résidence Services', re.IGNORECASE))
    print(f"Éléments avec 'Résidence Services': {len(residences_elements)}")

    for i, element in enumerate(residences_elements[:3]):
        print(f"\n--- ELEMENT {i+1} ---")
        print(f"Texte: {element.strip()}")
        
        # Remonter dans l'arbre pour trouver l'élément parent structurant
        parent = element.parent
        for level in range(5):
            if parent:
                classes = parent.get('class', [])
                id_attr = parent.get('id', '')
                print(f"Parent niveau {level}: <{parent.name}> classes={classes} id={id_attr}")
                parent = parent.parent
            else:
                break

    # Chercher des patterns de structure courants
    print("\n=== PATTERNS STRUCTURE ===")
    for class_pattern in ['residence', 'establishment', 'listing', 'item', 'card', 'bloc']:
        elements = soup.find_all(attrs={'class': re.compile(class_pattern, re.I)})
        if elements:
            print(f"{class_pattern}: {len(elements)} éléments trouvés")
            if len(elements) < 20:
                for elem in elements[:2]:
                    print(f"  <{elem.name}> class=\"{elem.get('class')}\"")

    # Chercher spécifiquement autour de "Frédéric Chopin"
    print("\n=== ANALYSE FRÉDÉRIC CHOPIN ===")
    chopin_elements = soup.find_all(string=re.compile(r'Frédéric Chopin', re.IGNORECASE))
    if chopin_elements:
        element = chopin_elements[0]
        print(f"Trouvé: {element.strip()}")
        
        # Analyser le contexte parent
        parent = element.parent
        while parent and parent.name != 'body':
            if parent.name in ['div', 'article', 'section'] and (parent.get('class') or parent.get('id')):
                print(f"Conteneur: <{parent.name}> class={parent.get('class')} id={parent.get('id')}")
                
                # Extraire le contenu de ce conteneur
                text_content = parent.get_text().strip()
                if len(text_content) < 500:  # Seulement si pas trop long
                    print(f"Contenu: {text_content}")
                break
            parent = parent.parent

if __name__ == "__main__":
    main()