"""
Script pour extraire les résidences services seniors directement depuis la page listing
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

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extraire toutes les résidences depuis les éléments h-card
    residences = soup.find_all('section', class_='result-entry h-card')
    print(f"\n🏢 {len(residences)} résidences trouvées")

    for i, residence in enumerate(residences, 1):
        print(f"\n--- RÉSIDENCE {i} ---")
        
        # Nom
        nom_element = residence.find('span', class_='p-name')
        nom = nom_element.get_text().strip() if nom_element else "Non trouvé"
        print(f"Nom: {nom}")
        
        # Adresse
        address_elements = residence.find_all(string=re.compile(r'\d{5}\s*-\s*\w+'))
        if address_elements:
            address_line = address_elements[0].strip()
            print(f"Adresse avec code postal: {address_line}")
            
            # Extraire code postal et commune
            match = re.search(r'(\d{5})\s*-\s*(.+)', address_line)
            if match:
                code_postal = match.group(1)
                commune = match.group(2).strip()
                print(f"Code postal: {code_postal}")
                print(f"Commune: {commune}")
        
        # Téléphone
        phone_elements = residence.find_all(string=re.compile(r'\d{2}\s*\d{2}\s*\d{2}\s*\d{2}\s*\d{2}'))
        if phone_elements:
            telephone = phone_elements[0].strip()
            print(f"Téléphone: {telephone}")
        
        # Site web (chercher les liens)
        links = residence.find_all('a', href=True)
        site_web = ""
        for link in links:
            href = link['href']
            if href.startswith('http') and 'pour-les-personnes-agees.gouv.fr' not in href:
                site_web = href
                print(f"Site web: {site_web}")
                break
        
        # Source et date de mise à jour
        source_elements = residence.find_all(string=re.compile(r'Source des données'))
        if source_elements:
            source_text = source_elements[0].strip()
            print(f"Source: {source_text}")
        
        date_elements = residence.find_all(string=re.compile(r'Mis à jour le'))
        if date_elements:
            date_text = date_elements[0].strip()
            print(f"Date MAJ: {date_text}")
            
        # Adresse complète
        all_text = residence.get_text()
        address_match = re.search(r'Adresse\s+([^\n]+(?:\n[^\n]+)*?)(?=\s*\d{2}\s+\d{2}|\s*Site internet|\s*Source)', all_text, re.MULTILINE)
        if address_match:
            adresse_complete = address_match.group(1).strip()
            print(f"Adresse complète: {adresse_complete}")

if __name__ == "__main__":
    main()