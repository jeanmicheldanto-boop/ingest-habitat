"""
Normalisation des noms propres : accents, casse, tirets, particules.
"""
import re
import unicodedata


def remove_accents(text: str) -> str:
    """Supprime les accents d'une chaîne."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalize_name(name: str) -> str:
    """
    Normalise un nom pour la déduplication : minuscules, sans accents, sans tirets.
    Ex: "Jean-Pierre Müller" → "jean pierre muller"
    """
    name = name.strip()
    name = remove_accents(name)
    name = name.lower()
    name = re.sub(r"[-_]", " ", name)
    name = re.sub(r"\s+", " ", name)
    return name


def clean_full_name(full_name: str) -> str:
    """
    Nettoie un nom complet :
    - Supprime les contenus entre parenthèses : "Dupont (dit Toto)" → "Dupont"
    - Supprime les contenus entre crochets
    - Supprime les points en milieu de nom type "LE MONNIER. DE GOUVILLE"
    - Normalise les espaces
    """
    # Supprimer contenus entre parenthèses ou crochets
    text = re.sub(r"[\(\[][^\)\]]*[\)\]]", "", full_name)
    # Supprimer points isolés (ex: "SERET. DE GOUVILLE" → "SERET DE GOUVILLE")
    text = re.sub(r"\s*\.\s*", " ", text)
    # Normaliser espaces
    text = re.sub(r"\s+", " ", text).strip()
    return text


def split_full_name(full_name: str) -> tuple[str, str]:
    """
    Tente de séparer prénom et nom depuis un nom complet.
    Convention : le premier mot est le prénom, le reste est le nom.
    Gère les prénoms composés (Jean-Pierre, Marie-Hélène).
    Retourne (prenom, nom).
    """
    full_name = clean_full_name(full_name)
    parts = full_name.strip().split()
    if not parts:
        return ("", "")
    if len(parts) == 1:
        # Un seul token : impossible de distinguer prénom/nom — on le met en nom
        return ("", parts[0])
    # Détecter les particules : de, du, de la, des, le, la, les
    particules = {"de", "du", "des", "le", "la", "les", "d'"}
    # Le prénom est le premier token (peut contenir un tiret : Jean-Pierre)
    prenom = parts[0]
    nom = " ".join(parts[1:])
    return (prenom, nom)


def email_slug(text: str) -> str:
    """
    Convertit une chaîne en slug utilisable dans un email :
    minuscules, sans accents, les espaces et tirets remplacés par des points.
    Ex: "Jean-Pierre" → "jean.pierre", "Müller" → "muller"
    """
    text = remove_accents(text)
    text = text.lower()
    text = re.sub(r"[-\s]", ".", text)
    text = re.sub(r"[^a-z0-9.]", "", text)
    text = re.sub(r"\.{2,}", ".", text)
    text = text.strip(".")
    return text


def build_email_variants(
    prenom: str,
    nom: str,
    domain: str,
    pattern: str = "prenom.nom",
    accents: str = "supprimés",
    tirets_noms: str = "point",
) -> list[str]:
    """
    Génère toutes les variantes d'email possibles pour un contact.

    Args:
        prenom: Prénom du contact (peut être composé: Jean-Pierre)
        nom: Nom du contact (peut être composé ou avoir une particule)
        domain: Domaine email (ex: seine-et-marne.fr)
        pattern: Pattern détecté (prenom.nom, pnom, p.nom, nom.prenom, ...)
        accents: "conservés" ou "supprimés"
        tirets_noms: "conservés", "point" ou "supprimés"

    Returns:
        Liste de variantes d'email, la première étant la plus probable.
    """
    def normalize_part(text: str, keep_accents: bool, tirets: str) -> str:
        if not keep_accents:
            text = remove_accents(text)
        text = text.lower()
        # Les espaces dans les noms composés ("Le Goff") → toujours remplacés par un point
        text = text.replace(" ", ".")
        if tirets == "conservés":
            pass  # garder le tiret
        elif tirets == "point":
            text = text.replace("-", ".")
        else:  # supprimés
            text = text.replace("-", "")
        return text

    keep_acc = accents == "conservés"

    p_std = normalize_part(prenom, keep_acc, tirets_noms)
    n_std = normalize_part(nom, keep_acc, tirets_noms)

    # Variantes prénom : avec tirets conservés, remplacés par point, supprimés, initiales
    p_hyphen_kept = normalize_part(prenom, keep_acc, "conservés")
    p_hyphen_point = normalize_part(prenom, keep_acc, "point")
    p_hyphen_removed = normalize_part(prenom, keep_acc, "supprimés")

    # Initiales : première lettre du prénom (avant et après tiret)
    initials_parts = re.split(r"[-\s]", remove_accents(prenom).lower())
    p_initial = initials_parts[0][0] if initials_parts and initials_parts[0] else ""
    p_initials_all = "".join(x[0] for x in initials_parts if x)

    # Nom : variantes avec particules (de, du, …)
    n_hyphen_kept = normalize_part(nom, keep_acc, "conservés")
    n_hyphen_point = normalize_part(nom, keep_acc, "point")
    n_hyphen_removed = normalize_part(nom, keep_acc, "supprimés")

    variants: list[str] = []

    def add(local: str) -> None:
        local = re.sub(r"\.{2,}", ".", local).strip(".")
        if local:
            email = f"{local}@{domain}"
            if email not in variants:
                variants.append(email)

    # Pattern principal (en premier)
    if pattern == "prenom.nom":
        add(f"{p_std}.{n_std}")
        add(f"{p_hyphen_point}.{n_std}")
        add(f"{p_hyphen_kept}.{n_std}")
        add(f"{p_hyphen_removed}.{n_std}")
    elif pattern == "pnom":
        add(f"{p_initial}{n_std}")
    elif pattern == "p.nom":
        add(f"{p_initial}.{n_std}")
    elif pattern == "nom.prenom":
        add(f"{n_std}.{p_std}")
    elif pattern == "prenom-nom":
        p_tmp = normalize_part(prenom, keep_acc, "conservés")
        n_tmp = normalize_part(nom, keep_acc, "conservés")
        add(f"{p_tmp}-{n_tmp}")
    else:
        # Défaut : prenom.nom
        add(f"{p_std}.{n_std}")

    # Variantes supplémentaires systématiques
    add(f"{p_std}.{n_std}")
    add(f"{p_hyphen_point}.{n_std}")
    add(f"{p_hyphen_removed}.{n_std}")
    add(f"{p_initial}.{n_std}")
    add(f"{p_initial}{n_std}")
    add(f"{p_initials_all}.{n_std}")
    add(f"{n_std}.{p_std}")

    # Cas noms avec particules : essayer sans la particule
    nom_parts = re.split(r"\s+", n_std)
    particules_set = {"de", "du", "des", "le", "la", "les", "d"}
    if nom_parts and nom_parts[0] in particules_set and len(nom_parts) > 1:
        n_no_part = ".".join(nom_parts[1:])
        add(f"{p_std}.{n_no_part}")
        add(f"{p_initial}.{n_no_part}")

    return variants
