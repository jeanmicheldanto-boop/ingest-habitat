# QUICK REFERENCE - Relance Emails V2

**Mise à jour**: 22 février 2026 | **Status**: Production Ready

---

## EN 2 MINUTES

### Test (Dry Run - RECOMMANDÉ)
```bash
python send_follow_up_emails.py
```
Résultat: 1085 emails préparés (simulation)

### Production (Envoi réel)
1. Ouvrir `send_emails_elasticmail.py`
2. Ligne 63: `DRY_RUN = False`
3. Sauvegarder
4. Exécuter: `python send_emails_elasticmail.py`
5. Attendre ~18 minutes

---

## STATISTIQUES

| Métrique | Valeur |
|----------|--------|
| Prospects total | 250 |
| Après filtrage | 246 (-4 Pas-de-Calais) |
| Avec contact email | 237 |
| Sans email | 9 |
| **Emails générés** | **1085** |
| Par prospect | 4.6 |
| Augmentation vs V1 | +375% |

---

## HIÉRARCHIE EMAILS (Priorité)

```
1. Email Dirigeant 1 (230) ⭐⭐⭐ BEST
2. Email Dirigeant 2 (210) ⭐⭐
3. Email Dirigeant 3 (200) ⭐
4. email_contact (172)
5. Email Organisation (174)
6. emails_generiques (168)
```

---

## FICHIERS CLÉS

**Input**:
- `outputs/prospection_250_FINAL_FORMATE_V2.xlsx` (enrichi)

**Scripts**:
- `send_follow_up_emails.py` (phase 1: préparation)
- `send_emails_elasticmail.py` (phase 2: envoi)

**Output** (après phase 1):
- `outputs/relance_emails/preparation_emails_*.csv`
- `outputs/relance_emails/preparation_emails_*.json`

**Output** (après phase 2):
- `outputs/relance_emails/envoi_rapport_*.json`

---

## CONFIGURATION

**DRY RUN MODE** (défaut, safe):
```python
# send_follow_up_emails.py
DRY_RUN = True  # Simulation
```

**PRODUCTION MODE**:
```python
# send_emails_elasticmail.py
dry_run = False  # Envoi réel
```

---

## LOGS IMPORTANTS

**Phase 1 - Préparation**:
- [FILE] Chargement du fichier
- [OK] X prospects chargés
- [SEARCH] Filtrage Pas-de-Calais
- [EMAIL] X emails préparés
- [CSV] Fichier sauvegardé
- [JSON] Fichier sauvegardé

**Phase 2 - Envoi**:
- Chaque email: [SIMULATION] ou [SEND] N/1085
- Toutes les 50: stats
- Final: Total, Sent, Failed, Success %

---

## TROUBLESHOOTING

**Problème**: Fichier Excel non trouvé
```
Vérifier: outputs/prospection_250_FINAL_FORMATE_V2.xlsx existe
```

**Problème**: Pas d'email généré
```
Vérifier: Que le fichier contient les colonnes:
  - Email Dirigeant 1/2/3
  - email_contact
  - Email Organisation
  - emails_generiques
```

**Problème**: Erreur d'encodage (Unicode)
```
Solution: Already fixed - usando [OK] instead of emojis
```

**Problème**: API Elasticmail indisponible
```
Statut: Check https://elasticmail.com/status
Action: Attendre la restauration du service
```

---

## DOCUMENTATION COMPLÈTE

Pour plus de détails, voir:
- **RELANCE_EMAILS_README.md** - Guide complet
- **MISE_A_JOUR_V2.md** - Détails des changements V1→V2
- **LOGIQUE_SCRIPTS_DETAIL.md** - Logique bas-niveau
- **INDEX_DOCUMENTATION.md** - Index de navigation
- **DOCUMENTATION_UPDATE_SUMMARY.md** - Résumé mise à jour

---

**Dernière mise à jour**: 22 février 2026 18:57  
**Test dry run**: [SUCCESS] 1085 emails prepared  
**Production status**: [READY] await API availability
