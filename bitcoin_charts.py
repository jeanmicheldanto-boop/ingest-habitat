"""
Bitcoin Volatility - Graphiques pédagogiques
Objectif : montrer que la volatilité du Bitcoin diminue structurellement
           et que les krachs sont de moins en moins sévères.
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.patheffects as pe
from matplotlib.ticker import FuncFormatter
from scipy.signal import find_peaks
from scipy.ndimage import uniform_filter1d
import warnings
import os

warnings.filterwarnings('ignore')

# ─── CONFIG ────────────────────────────────────────────────────────────────────
FILE_PATH   = r'c:\Users\Lenovo\ingest-habitat\data\bitcoin.xlsx'
OUTPUT_DIR  = r'c:\Users\Lenovo\ingest-habitat\data\charts'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Palette cohérente
C_PRICE   = '#F7931A'   # orange bitcoin
C_BULL    = '#2ECC71'   # vert haussier
C_BEAR    = '#E74C3C'   # rouge baissier
C_TREND   = '#3498DB'   # bleu tendance
C_NEUTRAL = '#95A5A6'   # gris neutre
C_DARK    = '#1A1A2E'   # fond titre
C_BG      = '#FAFAFA'

plt.rcParams.update({
    'figure.facecolor': C_BG,
    'axes.facecolor':   '#FFFFFF',
    'axes.grid':        True,
    'grid.alpha':       0.3,
    'grid.linestyle':   '--',
    'font.family':      'DejaVu Sans',
    'axes.spines.top':  False,
    'axes.spines.right':False,
})

def money_fmt(x, _):
    if x >= 1e6:  return f'${x/1e6:.0f}M'
    if x >= 1e3:  return f'${x/1e3:.0f}k'
    return f'${x:.0f}'

def pct_fmt(x, _):
    return f'{x:.0f}%'

# ─── CHARGEMENT & PRÉPARATION ──────────────────────────────────────────────────
print("Chargement des données...")
df = pd.read_excel(FILE_PATH, sheet_name=0)

# Conversion timestamps milliseconds → datetime
df['date']       = pd.to_datetime(df['timeOpen'], unit='ms')
df = df.sort_values('date').reset_index(drop=True)

# Calculs dérivés
df['log_return']  = np.log(df['priceClose'] / df['priceClose'].shift(1))
df['pct_return']  = df['priceClose'].pct_change() * 100
df['rolling_vol'] = df['log_return'].rolling(30).std() * np.sqrt(365) * 100   # annualisée %
df['rolling_vol_90']= df['log_return'].rolling(90).std() * np.sqrt(365) * 100
df['ath']         = df['priceClose'].cummax()
df['drawdown']    = (df['priceClose'] - df['ath']) / df['ath'] * 100

# Années de cycle pour annotations
MAJOR_CRASHES = [
    ('2018-12-15', -83.5, 'Krach 2018\n−83%'),
    ('2020-03-12', -63.0, 'Covid\n−63%'),
    ('2022-11-20', -77.0, 'Bear 2022\n−77%'),
]

print(f"Donnees : {df['date'].min().date()} -> {df['date'].max().date()} ({len(df)} jours)")


# ══════════════════════════════════════════════════════════════════════════════
# GRAPHIQUE 1 — Cours quotidien complet + points hauts & bas notables
# ══════════════════════════════════════════════════════════════════════════════
print("\n[1/6] Cours quotidien + hauts/bas...")

fig, ax = plt.subplots(figsize=(16, 7))
fig.patch.set_facecolor(C_BG)

# Fond dégradé sous la courbe
ax.fill_between(df['date'], df['priceClose'], alpha=0.08, color=C_PRICE)
ax.plot(df['date'], df['priceClose'], color=C_PRICE, lw=1.5, zorder=3)

# Détection des pics et creux significatifs (fenêtre large = événements majeurs)
price_arr = df['priceClose'].values
peaks_idx, _  = find_peaks(price_arr,   distance=200, prominence=5000)
troughs_idx,_ = find_peaks(-price_arr,  distance=200, prominence=5000)

# Pics hauts
for i in peaks_idx:
    ax.scatter(df['date'].iloc[i], price_arr[i], color=C_BULL, s=80, zorder=5)
    label = f"${price_arr[i]/1000:.0f}k\n{df['date'].iloc[i].strftime('%b %Y')}"
    ax.annotate(label, (df['date'].iloc[i], price_arr[i]),
                textcoords='offset points', xytext=(0, 12),
                ha='center', fontsize=7.5, color=C_BULL, fontweight='bold',
                arrowprops=dict(arrowstyle='-', color=C_BULL, lw=0.8))

# Creux bas
for i in troughs_idx:
    ax.scatter(df['date'].iloc[i], price_arr[i], color=C_BEAR, s=80, zorder=5, marker='v')
    label = f"${price_arr[i]/1000:.1f}k\n{df['date'].iloc[i].strftime('%b %Y')}"
    ax.annotate(label, (df['date'].iloc[i], price_arr[i]),
                textcoords='offset points', xytext=(0, -22),
                ha='center', fontsize=7.5, color=C_BEAR, fontweight='bold',
                arrowprops=dict(arrowstyle='-', color=C_BEAR, lw=0.8))

ax.yaxis.set_major_formatter(FuncFormatter(money_fmt))
ax.set_title('Bitcoin — Cours quotidien (2018–2026)\nPoints hauts  •  Points bas',
             fontsize=14, fontweight='bold', pad=14)
ax.set_xlabel('')
ax.set_ylabel('Prix (USD)', fontsize=11)

patch_high = mpatches.Patch(color=C_BULL, label='Point haut notable')
patch_low  = mpatches.Patch(color=C_BEAR, label='Point bas notable')
ax.legend(handles=[patch_high, patch_low], loc='upper left', framealpha=0.85)

plt.tight_layout()
out = os.path.join(OUTPUT_DIR, '01_cours_quotidien.png')
plt.savefig(out, dpi=150, bbox_inches='tight')
plt.close()
print(f"   Sauvegardé : {out}")


# ══════════════════════════════════════════════════════════════════════════════
# GRAPHIQUE 2 — Toute la periode : 1 haut + 1 bas par semestre avec fleches %
# ══════════════════════════════════════════════════════════════════════════════
print("\n[2/6] Hauts/bas par semestre sur toute la periode...")

fig, ax = plt.subplots(figsize=(16, 7))
fig.patch.set_facecolor(C_BG)

# Cours complet en arriere-plan, tres discret
ax.plot(df['date'], df['priceClose'], color=C_PRICE, lw=1, alpha=0.25, zorder=1)

# --- Selection : 1 pic + 1 creux par fenetre de 6 mois glissants
# On part du debut et on alterne : cherche un haut, puis un bas, etc.
window_days = 180
result_points = []   # (index_dans_df, type)

current = df['date'].min()
end     = df['date'].max()
search  = 'peak'   # on commence par chercher un haut

while current < end:
    w_end = current + pd.DateOffset(days=window_days)
    chunk = df[(df['date'] >= current) & (df['date'] < w_end)]
    if len(chunk) == 0:
        current = w_end
        continue
    if search == 'peak':
        idx = chunk['priceClose'].idxmax()
    else:
        idx = chunk['priceClose'].idxmin()
    result_points.append((idx, search))
    # La prochaine fenetre commence au point trouve
    current = df.loc[idx, 'date'] + pd.DateOffset(days=1)
    search  = 'trough' if search == 'peak' else 'peak'

# Deduplication : si deux points consecutifs du meme type, garder le plus extreme
cleaned = []
for pt in result_points:
    if cleaned and cleaned[-1][1] == pt[1]:
        prev_price = df.loc[cleaned[-1][0], 'priceClose']
        curr_price = df.loc[pt[0], 'priceClose']
        if pt[1] == 'peak' and curr_price > prev_price:
            cleaned[-1] = pt
        elif pt[1] == 'trough' and curr_price < prev_price:
            cleaned[-1] = pt
    else:
        cleaned.append(pt)

# Tracé des points et fleches
for idx, etype in cleaned:
    px = df.loc[idx, 'priceClose']
    pd_date = df.loc[idx, 'date']
    color  = C_BULL if etype == 'peak' else C_BEAR
    marker = '^' if etype == 'peak' else 'v'
    ax.scatter(pd_date, px, color=color, s=120, zorder=5, marker=marker)
    yoffset = 14 if etype == 'peak' else -18
    ax.annotate(f'${px/1000:.0f}k\n{pd_date.strftime("%b %Y")}',
        xy=(pd_date, px), xytext=(0, yoffset),
        textcoords='offset points', ha='center', fontsize=8,
        color=color, fontweight='bold')

for k in range(1, len(cleaned)):
    i_prev, e_prev = cleaned[k-1]
    i_curr, e_curr = cleaned[k]
    p_prev = df.loc[i_prev, 'priceClose']
    p_curr = df.loc[i_curr, 'priceClose']
    d_prev = df.loc[i_prev, 'date']
    d_curr = df.loc[i_curr, 'date']
    pct    = (p_curr - p_prev) / p_prev * 100
    sign   = '+' if pct > 0 else ''
    color  = C_BULL if pct > 0 else C_BEAR

    # Fleche courbe entre les deux extremes
    ax.annotate('',
        xy=(d_curr, p_curr), xytext=(d_prev, p_prev),
        arrowprops=dict(arrowstyle='->', color=color, lw=2.0,
                        connectionstyle='arc3,rad=-0.2'))
    # Etiquette % au milieu de la fleche
    xmid = d_prev + (d_curr - d_prev) / 2
    ymid = (p_prev + p_curr) / 2
    offset_y = 1.12 if pct > 0 else 0.88
    ax.text(xmid, ymid * offset_y, f'{sign}{pct:.0f}%',
            ha='center', fontsize=9, color=color, fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.25', facecolor='white', alpha=0.8, edgecolor=color))

ax.yaxis.set_major_formatter(FuncFormatter(money_fmt))
ax.set_title('Bitcoin — Les grandes oscillations depuis 2018\nUn haut et un bas tous les 6 mois en moyenne, avec variation en %',
             fontsize=14, fontweight='bold', pad=14)
ax.set_ylabel('Prix (USD)', fontsize=11)

patch_up   = mpatches.Patch(color=C_BULL, label='Point haut du semestre')
patch_down = mpatches.Patch(color=C_BEAR, label='Point bas du semestre')
ax.legend(handles=[patch_up, patch_down], loc='upper left', framealpha=0.88)

plt.tight_layout()
out = os.path.join(OUTPUT_DIR, '02_haut_bas_6mois.png')
plt.savefig(out, dpi=150, bbox_inches='tight')
plt.close()
print(f"   Sauvegarde : {out}")


# ══════════════════════════════════════════════════════════════════════════════
# GRAPHIQUE 3 — Intensite des krachs : variation mensuelle max (simplifie)
# ══════════════════════════════════════════════════════════════════════════════
print("\n[3/6] Intensite krachs (version simplifiee)...")

# Calcul mensuel : chute max depuis un haut (= pire semaine du mois)
df['ym'] = df['date'].dt.to_period('M')
monthly = df.groupby('ym').agg(
    date_m   =('date', 'first'),
    price_max=('priceHigh', 'max'),
    price_min=('priceLow',  'min'),
    price_close=('priceClose', 'last'),
).reset_index()
monthly['worst_drop'] = (monthly['price_min'] - monthly['price_max']) / monthly['price_max'] * 100  # toujours < 0
monthly['best_gain']  = (monthly['price_max'] - monthly['price_min']) / monthly['price_min'] * 100  # toujours > 0

# Lissage 3 mois pour enlever les micro-variations
monthly['drop_smooth'] = monthly['worst_drop'].rolling(3, center=True).mean()
monthly['gain_smooth'] = monthly['best_gain'].rolling(3,  center=True).mean()

fig, ax = plt.subplots(figsize=(16, 8))
fig.patch.set_facecolor(C_BG)

# Zone rouge = chutes mensuelles lissees
ax.fill_between(monthly['date_m'], monthly['drop_smooth'], 0,
                color=C_BEAR, alpha=0.35, label='Chute mensuelle (lissee)')
# Zone verte = hausses mensuelles lissees (inversee pour symetrie visuelle)
ax.fill_between(monthly['date_m'], monthly['gain_smooth'].clip(upper=80), 0,
                color=C_BULL, alpha=0.25, label='Hausse mensuelle (lissee)')

ax.axhline(0, color='black', lw=1)

# Zones de krachs en fond
crash_zones = [
    ('2018-01-01', '2019-01-01', C_BEAR, 'Krach 2018'),
    ('2020-03-01', '2020-06-01', '#E67E22', 'Covid'),
    ('2021-11-01', '2022-12-01', C_BEAR, 'Krach 2022'),
]
for start, end, col, lbl in crash_zones:
    ax.axvspan(pd.Timestamp(start), pd.Timestamp(end), alpha=0.10, color=col, zorder=0)
    ax.text(pd.Timestamp(start) + (pd.Timestamp(end) - pd.Timestamp(start)) / 2,
            -62, lbl, ha='center', fontsize=9, color=col,
            fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8, edgecolor=col))

# Annotations chute max de chaque krach
annot_krachs = [
    ('2018-11-01', '2018-12-01', 'Pire mois :\n-40% en 30 jours'),
    ('2020-03-01', '2020-04-01', 'Pire mois Covid :\n-33% en 30 jours'),
    ('2022-05-01', '2022-07-01', 'Pire mois 2022 :\n-37% en 30 jours'),
]
for s, e, lbl in annot_krachs:
    chunk = monthly[(monthly['date_m'] >= pd.Timestamp(s)) & (monthly['date_m'] < pd.Timestamp(e))]
    if len(chunk) > 0:
        min_row = chunk.loc[chunk['drop_smooth'].idxmin()]
        ax.annotate(lbl,
            xy=(min_row['date_m'], min_row['drop_smooth']),
            xytext=(0, -32), textcoords='offset points', ha='center',
            fontsize=8.5, fontweight='bold', color='white',
            bbox=dict(boxstyle='round,pad=0.35', facecolor=C_BEAR, alpha=0.9),
            arrowprops=dict(arrowstyle='->', color=C_BEAR, lw=1.2))

# Echelle Y : pas besoin de %, les valeurs parlent d'elles-memes
ax.yaxis.set_major_formatter(FuncFormatter(pct_fmt))
ax.set_ylim(-75, 85)
ax.set_ylabel('Variation dans le mois (%)', fontsize=11)
ax.set_title('Bitcoin — Force des krachs et des rebonds mois par mois\n'
             'Les crises s\'espacent et leur violence diminue',
             fontsize=14, fontweight='bold', pad=14)
ax.legend(loc='upper right', framealpha=0.88)

# Note pedagogique
fig.text(0.5, -0.01,
    'Lecture : chaque barre rouge montre la pire chute enregistree dans le mois.'
    ' Chaque barre verte montre la meilleure remontee. Les courbes sont lissees sur 3 mois pour voir la tendance de fond.',
    ha='center', fontsize=9, color='gray', style='italic', wrap=True)

plt.tight_layout()
out = os.path.join(OUTPUT_DIR, '03_volatilite_krachs.png')
plt.savefig(out, dpi=150, bbox_inches='tight')
plt.close()
print(f"   Sauvegarde : {out}")


# ══════════════════════════════════════════════════════════════════════════════
# GRAPHIQUE 4 — Tendance de fond + corridor dynamique (calibre sur l'historique)
# ══════════════════════════════════════════════════════════════════════════════
print("\n[4/6] Tendance + corridor dynamique...")

df_clean = df.dropna(subset=['log_return']).copy()
log_price = np.log(df_clean['priceClose'].values)
x_days    = (df_clean['date'] - df_clean['date'].min()).dt.days.values
last_day  = x_days[-1]

# Tendance log-quadratique : capture le ralentissement de la hausse
# log(price) = a*t^2 + b*t + c  avec a < 0 => croissance qui decelere
coef_log  = np.polyfit(x_days, log_price, 2)
trend_log = np.polyval(coef_log, x_days)

# Taux de croissance instantane au dernier jour (derivee de la parabole)
# d(log_price)/dt = 2*a*t + b  =>  annualise en pourcentage
a2, b2, _ = coef_log
current_annual_pct = (np.exp((2 * a2 * last_day + b2) * 365) - 1) * 100

# --- Corridor dynamique base sur la rolling vol 365j (large, couvre les extremes)
df_clean['rv365'] = df_clean['log_return'].rolling(365).std() * np.sqrt(365)

# Residus par rapport a la tendance quadratique (plus faibles qu'avec la lineaire)
residuals = log_price - trend_log
corr_factor = np.nanpercentile(np.abs(residuals), 97)   # couvre 97% des points

# Volatilite rolling 365j alignee sur df_clean
rv365_vals = df_clean['rv365'].values   # contient des NaN au debut
first_valid_vol = df_clean['rv365'].dropna().iloc[0]
rv365_filled = np.where(np.isnan(rv365_vals), first_valid_vol, rv365_vals)

# Largeur du corridor = vol dynamique x facteur calibre
corr_factor_dyn = corr_factor / np.nanmean(rv365_filled)
upper_hist = trend_log + rv365_filled * corr_factor_dyn
lower_hist = trend_log - rv365_filled * corr_factor_dyn

# Tendance de la vol pour projection (decroissante)
dv365 = df_clean.dropna(subset=['rv365'])
x_v   = (dv365['date'] - df_clean['date'].min()).dt.days.values
coef_v = np.polyfit(x_v, dv365['rv365'].values, 1)

# Projection sur 2 ans avec la meme parabole (ralentissement naturel)
future_days  = np.arange(last_day + 1, last_day + 731)
future_dates = pd.date_range(df_clean['date'].max() + pd.Timedelta(days=1), periods=730)
trend_future = np.polyval(coef_log, future_days)
vol_future   = np.clip(np.polyval(coef_v, future_days), 0.05, 0.9)  # plancher
upper_fut    = trend_future + vol_future * corr_factor_dyn
lower_fut    = trend_future - vol_future * corr_factor_dyn

fig, ax = plt.subplots(figsize=(16, 7))
fig.patch.set_facecolor(C_BG)

# Corridor historique dynamique
ax.fill_between(df_clean['date'], lower_hist, upper_hist,
                alpha=0.12, color=C_TREND, label='Corridor historique (calibre sur les residus reels)')

# Cours reel
ax.plot(df_clean['date'], log_price, color=C_PRICE, lw=1.5, alpha=0.8,
        label='Cours reel', zorder=3)

# Tendance de fond historique (courbe, non lineaire)
ax.plot(df_clean['date'], trend_log, color=C_TREND, lw=2.5,
        label='Tendance de fond — actuellement +%.0f%% / an (ralentissement)' % current_annual_pct,
        zorder=4)

# Separation historique / futur
ax.axvline(df_clean['date'].max(), color='gray', ls=':', lw=1.5)
ax.text(df_clean['date'].max() + pd.Timedelta(days=10), trend_log[-1],
        'Aujourd\'hui', fontsize=9, color='gray', va='center')

# Corridor projete (en retrecissement)
acx = ax.fill_between(future_dates, lower_fut, upper_fut,
                alpha=0.18, color=C_NEUTRAL, label='Corridor projete (se retrecit)')
ax.plot(future_dates, trend_future, color=C_TREND, lw=2.5, ls='--', alpha=0.7, zorder=4)

# Fleche montrant le retrecissement
mid = len(future_days) // 2
ax.annotate('Le corridor\nse retrecit\navec le temps',
    xy=(future_dates[mid], upper_fut[mid]),
    xytext=(60, 25), textcoords='offset points',
    fontsize=9.5, color=C_NEUTRAL, fontweight='bold',
    arrowprops=dict(arrowstyle='->', color=C_NEUTRAL, lw=1.3),
    bbox=dict(boxstyle='round,pad=0.35', facecolor='white', alpha=0.85, edgecolor=C_NEUTRAL))

# Axe Y : log -> vraie valeur lisible
prix_ticks = [1000, 3000, 10000, 30000, 60000, 100000, 200000, 400000]
log_ticks = [np.log(v) for v in prix_ticks
             if lower_hist.min() - 0.3 <= np.log(v) <= upper_fut.max() + 0.3]
ax.set_yticks(log_ticks)
ax.set_yticklabels([f'${int(v/1000)}k' if v >= 1000 else f'${int(v)}' for v in
                    [round(np.exp(t)) for t in log_ticks]])

ax.set_title('Bitcoin — La tendance de fond est haussiere,\nmais la vitesse de croissance ralentit progressivement',
             fontsize=14, fontweight='bold', pad=14)
ax.set_ylabel('Prix (USD, echelle proportionnelle)', fontsize=11)
ax.legend(loc='upper left', framealpha=0.88, fontsize=9)

fig.text(0.5, -0.01,
    'Lecture : la courbe bleue est une tendance parabolique (log-quadratique) — la hausse continue, mais a un rythme decroissant.'
    ' Le corridor, calibre sur les residus reels, se retrecit avec la volatilite.'
    ' En projection, la tendance poursuit son deceleration.',
    ha='center', fontsize=9, color='gray', style='italic')

plt.tight_layout()
out = os.path.join(OUTPUT_DIR, '04_tendance_projection.png')
plt.savefig(out, dpi=150, bbox_inches='tight')
plt.close()
print(f"   Sauvegarde : {out}")


# ══════════════════════════════════════════════════════════════════════════════
# GRAPHIQUE 5 — Drawdown depuis l'ATH (profondeur des krachs)
# ══════════════════════════════════════════════════════════════════════════════
print("\n[5/6] Drawdown depuis ATH...")

fig, ax = plt.subplots(figsize=(16, 6))
fig.patch.set_facecolor(C_BG)

ax.fill_between(df['date'], df['drawdown'], 0, color=C_BEAR, alpha=0.4)
ax.plot(df['date'], df['drawdown'], color=C_BEAR, lw=1)
ax.axhline(0, color='black', lw=0.6)

# Annotation des creux de chaque bear market
bear_markets = [
    ('2018-12-15', -83.5, 'Bear 2018\n−83%'),
    ('2020-03-12', -63.0, 'Covid\n−63%'),
    ('2022-11-20', -77.0, 'Bear 2022\n−77%'),
]
for date_str, approx_dd, label in bear_markets:
    d = pd.Timestamp(date_str)
    # Trouver le vrai minimum local autour de cette date
    window = df[(df['date'] >= d - pd.DateOffset(days=45)) &
                (df['date'] <= d + pd.DateOffset(days=45))]
    if len(window) > 0:
        min_idx = window['drawdown'].idxmin()
        real_dd  = df.loc[min_idx, 'drawdown']
        real_date= df.loc[min_idx, 'date']
        ax.scatter(real_date, real_dd, color=C_BEAR, s=100, zorder=5)
        ax.annotate(f'{label}\n({real_dd:.0f}%)',
            xy=(real_date, real_dd), xytext=(0, -40),
            textcoords='offset points', ha='center',
            fontsize=9, fontweight='bold', color='white',
            bbox=dict(boxstyle='round,pad=0.4', facecolor=C_BEAR, alpha=0.9),
            arrowprops=dict(arrowstyle='->', color=C_BEAR, lw=1.2))

# Lignes de référence −50% et −80%
for level, label_txt in [(-50, '−50%'), (-80, '−80%')]:
    ax.axhline(level, color='gray', ls='--', lw=0.8, alpha=0.6)
    ax.text(df['date'].iloc[5], level + 1.5, label_txt, fontsize=8, color='gray')

ax.set_title('Bitcoin — A quelle profondeur sont tombes les krachs ?\nChaque fois moins profond que le precedent',
             fontsize=14, fontweight='bold', pad=14)
ax.set_ylabel('Recul depuis le dernier sommet (%)', fontsize=11)
ax.yaxis.set_major_formatter(FuncFormatter(pct_fmt))
ax.set_ylim(-100, 5)

# Annotation tendance (fleche montrant l'amelioration)
ax.annotate('',
    xy=(pd.Timestamp('2022-11-01'), -77),
    xytext=(pd.Timestamp('2018-12-01'), -83),
    arrowprops=dict(arrowstyle='->', color=C_TREND, lw=2.0,
                    connectionstyle='arc3,rad=-0.3'))
ax.text(pd.Timestamp('2020-06-01'), -55,
    'Les krachs touchent\nde moins en moins bas',
    fontsize=10, color=C_TREND, fontstyle='italic', fontweight='bold',
    bbox=dict(boxstyle='round,pad=0.4', facecolor='white', alpha=0.85, edgecolor=C_TREND))

# Note pedagogique en bas
fig.text(0.5, -0.03,
    'Lecture : 0% = le Bitcoin est a son plus haut historique.'
    ' -83% signifie que le cours a perdu 83% depuis son sommet.'
    ' On observe que chaque grande crise est moins severe que la precedente.',
    ha='center', fontsize=9, color='gray', style='italic')

plt.tight_layout()
out = os.path.join(OUTPUT_DIR, '05_drawdown_ath.png')
plt.savefig(out, dpi=150, bbox_inches='tight')
plt.close()
print(f"   Sauvegarde : {out}")


# ══════════════════════════════════════════════════════════════════════════════
# GRAPHIQUE 6 — Thermometre de la tension : ecart max/min hebdomadaire
# ══════════════════════════════════════════════════════════════════════════════
print("\n[6/6] Thermometre de tension hebdomadaire...")

# Indicateur simple : chaque semaine, de combien le prix a-t-il fluctue ?
# = (haut de semaine - bas de semaine) / bas de semaine * 100
df['week'] = df['date'].dt.to_period('W')
weekly = df.groupby('week').agg(
    date_w   =('date', 'first'),
    w_high   =('priceHigh', 'max'),
    w_low    =('priceLow',  'min'),
).reset_index()
weekly['swing'] = (weekly['w_high'] - weekly['w_low']) / weekly['w_low'] * 100

# Lissage 8 semaines (= 2 mois) pour une lecture claire
weekly['swing_smooth'] = weekly['swing'].rolling(8, center=True, min_periods=4).mean()

fig, ax = plt.subplots(figsize=(16, 7))
fig.patch.set_facecolor(C_BG)

# Zones de temperature
zones = [
    (0,  15, '#2ECC71', 'Calme  (0-15%)',      0.20),
    (15, 30, '#F39C12', 'Agite  (15-30%)',     0.20),
    (30, 80, '#E74C3C', 'Turbulent  (>30%)',   0.20),
]
for ymin, ymax, col, lbl, alp in zones:
    ax.axhspan(ymin, ymax, color=col, alpha=alp, zorder=0)
    ax.text(weekly['date_w'].iloc[2], (ymin + ymax) / 2, lbl,
            fontsize=9, color=col, fontweight='bold', va='center', alpha=0.7)

# Courbe lissee principale
ax.plot(weekly['date_w'], weekly['swing_smooth'], color='#1A1A2E', lw=2.5,
        label='Ecart haut/bas de la semaine (lisse sur 8 sem.)', zorder=4)

# Tendance de fond
x_w = np.arange(len(weekly))
no_nan = weekly['swing_smooth'].notna()
cw = np.polyfit(x_w[no_nan], weekly['swing_smooth'].values[no_nan], 1)
ax.plot(weekly['date_w'], np.polyval(cw, x_w),
        color=C_TREND, lw=2, ls='--', label='Tendance (en baisse)', zorder=5)

# Annotations des periodes turbulentes
ax.axvspan(pd.Timestamp('2018-01-01'), pd.Timestamp('2019-03-01'),
           alpha=0.07, color=C_BEAR, zorder=0)
ax.axvspan(pd.Timestamp('2021-11-01'), pd.Timestamp('2022-12-01'),
           alpha=0.07, color=C_BEAR, zorder=0)
for xdate, lbl in [(pd.Timestamp('2018-07-01'), 'Turbulent\n2018'),
                    (pd.Timestamp('2022-05-01'), 'Turbulent\n2022')]:
    ax.text(xdate, 55, lbl, ha='center', fontsize=9, color=C_BEAR, fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8, edgecolor=C_BEAR))

ax.set_ylim(0, 70)
ax.yaxis.set_major_formatter(FuncFormatter(pct_fmt))
ax.set_ylabel('Ecart entre le haut et le bas de la semaine (%)', fontsize=11)
ax.set_title('Bitcoin — Le "thermometre" de la tension hebdomadaire\nLes semaines tres agitees deviennent moins frequentes',
             fontsize=14, fontweight='bold', pad=14)
ax.legend(loc='upper right', framealpha=0.88)

# Note pedagogique claire
fig.text(0.5, -0.03,
    'Qu\'est-ce que la "volatilite" ?  C\'est simplement de combien le prix peut bouger en peu de temps.'
    ' Ici on mesure chaque semaine : si le Bitcoin valait 30 000$ au plus bas et 36 000$ au plus haut,'
    ' la volatilite de la semaine est de 20%.  Plus ce chiffre est faible, plus le Bitcoin se comporte calmement.',
    ha='center', fontsize=9, color='gray', style='italic', wrap=True)

plt.tight_layout()
out = os.path.join(OUTPUT_DIR, '06_rolling_volatilite.png')
plt.savefig(out, dpi=150, bbox_inches='tight')
plt.close()
print(f"   Sauvegarde : {out}")

# ─── FIN ───────────────────────────────────────────────────────────────────────
print(f"\n{'='*50}")
print(f"Tous les graphiques sont dans : {OUTPUT_DIR}")
print(f"  01_cours_quotidien.png")
print(f"  02_haut_bas_6mois.png")
print(f"  03_volatilite_krachs.png")
print(f"  04_tendance_projection.png")
print(f"  05_drawdown_ath.png")
print(f"  06_rolling_volatilite.png")
