"""
Sellvance — Motor de Análise de Campanhas com IA
Analisa métricas e gera recomendações automáticas
"""

def calc_metrics(c):
    """Calcula todas as métricas derivadas de uma campanha."""
    spend       = c['spend'] or 0
    revenue     = c['revenue'] or 0
    clicks      = c['clicks'] or 0
    impressions = c['impressions'] or 1
    conversions = c['conversions'] or 0
    leads       = c['leads'] or 0
    reach       = c['reach'] or 1

    roas    = round(revenue / spend, 2)        if spend > 0 else 0
    roi     = round((revenue - spend) / spend * 100, 1) if spend > 0 else 0
    cpc     = round(spend / clicks, 2)         if clicks > 0 else 0
    ctr     = round(clicks / impressions * 100, 2)
    cpl     = round(spend / leads, 2)          if leads > 0 else 0
    cpa     = round(spend / conversions, 2)    if conversions > 0 else 0
    freq    = round(impressions / reach, 1)    if reach > 0 else 0
    cvr     = round(conversions / clicks * 100, 2) if clicks > 0 else 0
    ticket  = round(revenue / conversions, 2)  if conversions > 0 else 0

    return {
        'roas': roas, 'roi': roi, 'cpc': cpc, 'ctr': ctr,
        'cpl': cpl, 'cpa': cpa, 'freq': freq, 'cvr': cvr,
        'ticket': ticket,
    }


# Benchmarks por plataforma e objetivo
BENCHMARKS = {
    'meta': {
        'conversao': {'roas_min': 3.0, 'ctr_min': 1.2, 'cpc_max': 3.5,  'freq_max': 4.0, 'cpa_max': 80},
        'lead_gen':  {'roas_min': 0,   'ctr_min': 1.5, 'cpc_max': 2.5,  'freq_max': 3.5, 'cpl_max': 25},
        'video':     {'roas_min': 0,   'ctr_min': 0.5, 'cpc_max': 1.5,  'freq_max': 5.0, 'cpa_max': 999},
        'default':   {'roas_min': 2.5, 'ctr_min': 1.0, 'cpc_max': 4.0,  'freq_max': 4.0, 'cpa_max': 100},
    },
    'google': {
        'shopping':  {'roas_min': 4.0, 'ctr_min': 0.8, 'cpc_max': 2.5,  'freq_max': 0,   'cpa_max': 60},
        'search':    {'roas_min': 3.5, 'ctr_min': 5.0, 'cpc_max': 4.0,  'freq_max': 0,   'cpa_max': 70},
        'pmax':      {'roas_min': 4.0, 'ctr_min': 2.0, 'cpc_max': 3.0,  'freq_max': 0,   'cpa_max': 65},
        'display':   {'roas_min': 1.5, 'ctr_min': 0.3, 'cpc_max': 1.5,  'freq_max': 0,   'cpa_max': 120},
        'default':   {'roas_min': 3.0, 'ctr_min': 2.0, 'cpc_max': 3.5,  'freq_max': 0,   'cpa_max': 80},
    },
    'tiktok': {
        'shop':      {'roas_min': 4.0, 'ctr_min': 1.5, 'cpc_max': 2.0,  'freq_max': 0,   'cpa_max': 50},
        'video':     {'roas_min': 2.0, 'ctr_min': 1.0, 'cpc_max': 1.5,  'freq_max': 0,   'cpa_max': 80},
        'default':   {'roas_min': 3.0, 'ctr_min': 1.2, 'cpc_max': 2.0,  'freq_max': 0,   'cpa_max': 60},
    }
}


def get_benchmark(platform, objective):
    plat = BENCHMARKS.get(platform, BENCHMARKS['meta'])
    return plat.get(objective, plat.get('default', {}))


def score_campaign(c, m):
    """
    Pontua campanha de 0-100 com base nas métricas vs benchmarks.
    Retorna score, status, problemas e sugestões.
    """
    platform  = c['platform']
    objective = c['objective']
    bench     = get_benchmark(platform, objective)
    problems  = []
    suggestions = []
    score     = 100

    # ── ROAS ─────────────────────────────────────────────────
    roas_min = bench.get('roas_min', 0)
    if roas_min > 0:
        if m['roas'] < roas_min * 0.5:
            score -= 35
            problems.append(f"ROAS crítico ({m['roas']}x) — muito abaixo do mínimo de {roas_min}x")
            suggestions.append("Pausar imediatamente e revisar público-alvo e criativo")
        elif m['roas'] < roas_min:
            score -= 18
            problems.append(f"ROAS abaixo do ideal ({m['roas']}x vs mínimo {roas_min}x)")
            suggestions.append("Testar novos criativos e refinar segmentação de público")
        elif m['roas'] >= roas_min * 1.5:
            score += 5  # bônus
            suggestions.append(f"Excelente ROAS! Considere aumentar o budget em 20-30%")

    # ── CTR ──────────────────────────────────────────────────
    ctr_min = bench.get('ctr_min', 0)
    if ctr_min > 0:
        if m['ctr'] < ctr_min * 0.5:
            score -= 20
            problems.append(f"CTR muito baixo ({m['ctr']}%) — criativo não está atraindo cliques")
            suggestions.append("Renovar criativos urgentemente — testar vídeos curtos e UGC")
        elif m['ctr'] < ctr_min:
            score -= 10
            problems.append(f"CTR abaixo do benchmark ({m['ctr']}% vs {ctr_min}% esperado)")
            suggestions.append("Testar novas copies e thumbnails com mais urgência/CTA claro")

    # ── CPC ──────────────────────────────────────────────────
    cpc_max = bench.get('cpc_max', 0)
    if cpc_max > 0 and m['cpc'] > 0:
        if m['cpc'] > cpc_max * 1.5:
            score -= 15
            problems.append(f"CPC elevado (R${m['cpc']}) — custo por clique acima do ideal")
            suggestions.append("Ampliar público para reduzir competição por leilão")
        elif m['cpc'] > cpc_max:
            score -= 8
            suggestions.append(f"CPC pode ser otimizado (R${m['cpc']} vs ideal R${cpc_max})")

    # ── FREQUÊNCIA (Meta) ─────────────────────────────────────
    freq_max = bench.get('freq_max', 0)
    if freq_max > 0 and m['freq'] > freq_max:
        score -= 15
        problems.append(f"Frequência alta ({m['freq']}x) — público saturado")
        suggestions.append("Ampliar público ou pausar por 7 dias para resetar saturação")

    # ── CPA ──────────────────────────────────────────────────
    cpa_max = bench.get('cpa_max', 0)
    if cpa_max > 0 and m['cpa'] > 0:
        if m['cpa'] > cpa_max * 1.5:
            score -= 20
            problems.append(f"CPA muito alto (R${m['cpa']}) — custo por conversão insustentável")
            suggestions.append("Revisar funil de vendas e otimizar landing page")
        elif m['cpa'] > cpa_max:
            score -= 10
            problems.append(f"CPA acima do ideal (R${m['cpa']} vs máximo R${cpa_max})")
            suggestions.append("Otimizar para conversões de maior valor")

    # ── Score final ───────────────────────────────────────────
    score = max(0, min(100, score))

    if score >= 75:
        status_label = 'Ótima'
        status_color = 'green'
        should_pause = False
        ai_action    = '✅ Campanha performando bem. Monitorar e escalar.'
    elif score >= 50:
        status_label = 'Regular'
        status_color = 'yellow'
        should_pause = False
        ai_action    = '⚠️ Campanha precisa de otimizações. Aplicar sugestões antes de escalar.'
    elif score >= 30:
        status_label = 'Ruim'
        status_color = 'orange'
        should_pause = False
        ai_action    = '🔴 Performance abaixo do aceitável. Aplicar correções urgentes.'
    else:
        status_label = 'Crítica'
        status_color = 'red'
        should_pause = True
        ai_action    = '🚨 Recomendo pausar esta campanha — está gerando prejuízo.'

    return {
        'score': score,
        'status_label': status_label,
        'status_color': status_color,
        'should_pause': should_pause,
        'ai_action': ai_action,
        'problems': problems,
        'suggestions': suggestions,
    }


def analyze_all(campaigns):
    """Analisa todas as campanhas e retorna ranking + insights globais."""
    results = []
    camps_list = [dict(c) for c in campaigns]
    total_spend   = sum(c.get('spend', 0) for c in camps_list)
    total_revenue = sum(c.get('revenue', 0) for c in camps_list)

    for c in camps_list:
        m    = calc_metrics(c)
        anal = score_campaign(c, m)
        results.append({**c, **m, **anal})

    # Ordenar por score desc
    results.sort(key=lambda x: x['score'], reverse=True)

    # Insights globais
    pausar = [r for r in results if r['should_pause']]
    escalar = [r for r in results if r['score'] >= 75 and r['status'] != 'paused']
    global_roas = round(total_revenue / total_spend, 2) if total_spend > 0 else 0

    insights = []
    if pausar:
        nomes = ', '.join(r['name'] for r in pausar)
        insights.append({
            'type': 'danger',
            'icon': '🚨',
            'title': f"{len(pausar)} campanha(s) recomendada(s) para pausa",
            'text': nomes,
        })
    if escalar:
        best = escalar[0]
        insights.append({
            'type': 'success',
            'icon': '🚀',
            'title': f"Melhor campanha: {best['name']}",
            'text': f"ROAS {best['roas']}x — Considere aumentar budget em 20-30%",
        })
    if global_roas < 3:
        insights.append({
            'type': 'warning',
            'icon': '⚠️',
            'title': f"ROAS global baixo ({global_roas}x)",
            'text': "Portfolio de campanhas abaixo do ideal. Realoque budget para campanhas top.",
        })
    elif global_roas >= 5:
        insights.append({
            'type': 'success',
            'icon': '💰',
            'title': f"ROAS global excelente ({global_roas}x)",
            'text': "Momento ideal para aumentar investimento total.",
        })

    return results, insights, global_roas
