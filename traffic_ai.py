"""
Sellvance — Motor de Análise de Campanhas com IA
Analisa métricas e gera recomendações automáticas com plano de ação detalhado
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
    Retorna score, status, problemas e sugestões detalhadas.
    """
    platform  = c['platform']
    objective = c.get('objective', 'default')
    bench     = get_benchmark(platform, objective)
    problems  = []
    suggestions = []  # list of dicts with title, desc, actions, impact, priority
    score     = 100

    spend   = c.get('spend', 0) or 0
    revenue = c.get('revenue', 0) or 0

    # ── ROAS ─────────────────────────────────────────────────
    roas_min = bench.get('roas_min', 0)
    if roas_min > 0:
        if m['roas'] < roas_min * 0.5:
            score -= 35
            problems.append(f"ROAS critico ({m['roas']}x) — muito abaixo do minimo de {roas_min}x")
            suggestions.append({
                'title': 'Pausar e Revisar Estrategia',
                'desc': f"O ROAS esta em {m['roas']}x, muito abaixo do minimo aceitavel de {roas_min}x. A campanha esta gerando prejuizo.",
                'actions': [
                    'Pausar a campanha imediatamente para parar o gasto',
                    'Revisar o publico-alvo — pode estar muito amplo ou desalinhado',
                    'Analisar os criativos — testar novas imagens/videos com proposta de valor clara',
                    'Revisar a landing page — verificar se a pagina de destino converte bem',
                    'Reativar com budget reduzido (50%) apos ajustes para testar'
                ],
                'impact': 'Alto',
                'priority': 'Urgente',
                'expected': f"Parar o prejuizo imediato de R$ {spend - revenue:.0f} e recomecar com base solida"
            })
        elif m['roas'] < roas_min:
            score -= 18
            problems.append(f"ROAS abaixo do ideal ({m['roas']}x vs minimo {roas_min}x)")
            suggestions.append({
                'title': 'Otimizar ROAS com Novos Criativos',
                'desc': f"O ROAS esta em {m['roas']}x, abaixo do benchmark de {roas_min}x. Precisa melhorar para ser rentavel.",
                'actions': [
                    f"Criar 3-5 variacoes de criativos novos (imagens e videos curtos de 15s)",
                    'Refinar segmentacao — excluir publicos que nao convertem',
                    'Testar copys diferentes com foco em beneficios e urgencia',
                    'Configurar teste A/B entre criativos novos vs atual',
                    'Monitorar por 3-5 dias antes de decidir proximo passo'
                ],
                'impact': 'Alto',
                'priority': 'Esta semana',
                'expected': f"Aumentar ROAS de {m['roas']}x para pelo menos {roas_min}x nas proximas 2 semanas"
            })
        elif m['roas'] >= roas_min * 1.5:
            score += 5
            new_budget = round(spend * 1.25, 2)
            suggestions.append({
                'title': 'Escalar Budget em 25%',
                'desc': f"ROAS excelente de {m['roas']}x! Esta muito acima do benchmark de {roas_min}x. Momento ideal para escalar.",
                'actions': [
                    f"Aumentar o budget diario de R$ {spend:.0f} para R$ {new_budget:.0f} (+25%)",
                    'Duplicar o conjunto de anuncios com o mesmo publico para testar estabilidade',
                    'Criar publico Lookalike baseado nos compradores desta campanha',
                    'Manter os criativos atuais que estao performando bem',
                    'Acompanhar ROAS diariamente por 5 dias — se cair abaixo de {:.1f}x, reverter'.format(roas_min)
                ],
                'impact': 'Alto',
                'priority': 'Agora',
                'expected': f"Aumentar receita em ~25% mantendo ROAS acima de {roas_min}x"
            })

    # ── CTR ──────────────────────────────────────────────────
    ctr_min = bench.get('ctr_min', 0)
    if ctr_min > 0:
        if m['ctr'] < ctr_min * 0.5:
            score -= 20
            problems.append(f"CTR muito baixo ({m['ctr']}%) — criativo nao esta atraindo cliques")
            suggestions.append({
                'title': 'Renovar Criativos Urgente',
                'desc': f"CTR de apenas {m['ctr']}% (benchmark: {ctr_min}%). O publico esta vendo o anuncio mas nao clica.",
                'actions': [
                    'Criar 3 novos criativos em video curto (Reels/Stories de 15s)',
                    'Testar UGC (conteudo gerado por usuario) como formato principal',
                    'Mudar a copy do anuncio: usar pergunta ou numero impactante no inicio',
                    'Adicionar CTA mais claro e visivel no criativo ("Compre Agora", "Veja Oferta")',
                    'Testar carrossel com beneficios do produto vs imagem unica'
                ],
                'impact': 'Alto',
                'priority': 'Urgente',
                'expected': f"Subir CTR de {m['ctr']}% para pelo menos {ctr_min}% em 7 dias"
            })
        elif m['ctr'] < ctr_min:
            score -= 10
            problems.append(f"CTR abaixo do benchmark ({m['ctr']}% vs {ctr_min}% esperado)")
            suggestions.append({
                'title': 'Melhorar Copy e Thumbnails',
                'desc': f"CTR de {m['ctr']}% esta abaixo do ideal de {ctr_min}%. Pequenos ajustes podem melhorar.",
                'actions': [
                    'Testar 2-3 novas copies com gatilhos de urgencia ou escassez',
                    'Trocar thumbnail/imagem principal por uma com mais contraste e texto bold',
                    'Adicionar emoji ou elemento visual chamativo na copy',
                    'Testar formato diferente (video se for imagem, carrossel se for video)'
                ],
                'impact': 'Medio',
                'priority': 'Esta semana',
                'expected': f"Aumentar CTR de {m['ctr']}% para {ctr_min}%+"
            })

    # ── CPC ──────────────────────────────────────────────────
    cpc_max = bench.get('cpc_max', 0)
    if cpc_max > 0 and m['cpc'] > 0:
        if m['cpc'] > cpc_max * 1.5:
            score -= 15
            problems.append(f"CPC elevado (R${m['cpc']}) — custo por clique acima do ideal")
            suggestions.append({
                'title': 'Reduzir CPC Expandindo Publico',
                'desc': f"CPC de R$ {m['cpc']} esta {round((m['cpc']/cpc_max - 1)*100)}% acima do ideal de R$ {cpc_max}.",
                'actions': [
                    'Ampliar o tamanho do publico-alvo para reduzir competicao no leilao',
                    'Remover restricoes de idade/genero desnecessarias',
                    'Testar posicionamentos automaticos em vez de so Feed',
                    f"Definir lance maximo de R$ {cpc_max:.2f} para controlar custo",
                    'Considerar mudar objetivo para "Alcance" temporariamente para baratear'
                ],
                'impact': 'Medio',
                'priority': 'Esta semana',
                'expected': f"Reduzir CPC de R$ {m['cpc']:.2f} para R$ {cpc_max:.2f}"
            })
        elif m['cpc'] > cpc_max:
            score -= 8
            suggestions.append({
                'title': 'Otimizar CPC',
                'desc': f"CPC de R$ {m['cpc']} pode ser melhorado (ideal: R$ {cpc_max}).",
                'actions': [
                    'Ampliar publico em 10-20% adicionando interesses relacionados',
                    'Testar posicionamento automatico para encontrar canais mais baratos',
                    f"Monitorar se CPC cai abaixo de R$ {cpc_max:.2f} em 3 dias"
                ],
                'impact': 'Baixo',
                'priority': 'Proximo ciclo',
                'expected': f"Reducao de ~{round((1 - cpc_max/m['cpc'])*100)}% no CPC"
            })

    # ── FREQUENCIA (Meta) ─────────────────────────────────────
    freq_max = bench.get('freq_max', 0)
    if freq_max > 0 and m['freq'] > freq_max:
        score -= 15
        problems.append(f"Frequencia alta ({m['freq']}x) — publico saturado")
        suggestions.append({
            'title': 'Combater Saturacao de Publico',
            'desc': f"Frequencia de {m['freq']}x significa que cada pessoa viu o anuncio {m['freq']} vezes. Acima de {freq_max}x causa fadiga.",
            'actions': [
                'Criar publico Lookalike novo para expandir o alcance',
                'Excluir quem ja converteu nos ultimos 30 dias',
                'Pausar a campanha por 5-7 dias para resetar a fadiga',
                'Alternar entre 2-3 criativos diferentes para manter novidade',
                'Aumentar o tamanho do publico adicionando novos interesses'
            ],
            'impact': 'Alto',
            'priority': 'Urgente',
            'expected': f"Reduzir frequencia de {m['freq']}x para abaixo de {freq_max}x"
        })

    # ── CPA ──────────────────────────────────────────────────
    cpa_max = bench.get('cpa_max', 0)
    if cpa_max > 0 and m['cpa'] > 0:
        if m['cpa'] > cpa_max * 1.5:
            score -= 20
            problems.append(f"CPA muito alto (R${m['cpa']}) — custo por conversao insustentavel")
            suggestions.append({
                'title': 'Reduzir Custo por Conversao',
                'desc': f"CPA de R$ {m['cpa']:.0f} esta muito acima do maximo de R$ {cpa_max}. Cada venda custa caro demais.",
                'actions': [
                    'Revisar a landing page — melhorar velocidade, CTA e prova social',
                    'Adicionar depoimentos e avaliacoes de clientes na pagina',
                    'Simplificar o checkout — reduzir campos e adicionar opcoes de pagamento',
                    'Testar oferta diferente (desconto, frete gratis, brinde)',
                    'Excluir publicos frios e focar em retargeting (visitantes do site)'
                ],
                'impact': 'Alto',
                'priority': 'Urgente',
                'expected': f"Reduzir CPA de R$ {m['cpa']:.0f} para abaixo de R$ {cpa_max}"
            })
        elif m['cpa'] > cpa_max:
            score -= 10
            problems.append(f"CPA acima do ideal (R${m['cpa']} vs maximo R${cpa_max})")
            suggestions.append({
                'title': 'Otimizar Funil de Conversao',
                'desc': f"CPA de R$ {m['cpa']:.0f} esta um pouco acima do ideal de R$ {cpa_max}.",
                'actions': [
                    'Otimizar landing page para conversao (botao mais visivel, menos distracao)',
                    'Criar campanha de retargeting para visitantes que nao compraram',
                    'Testar oferta ou incentivo para aumentar taxa de conversao'
                ],
                'impact': 'Medio',
                'priority': 'Esta semana',
                'expected': f"Reduzir CPA em ~{round((1 - cpa_max/m['cpa'])*100)}%"
            })

    # ── Score final ───────────────────────────────────────────
    score = max(0, min(100, score))

    if score >= 75:
        status_label = 'Otima'
        status_color = 'green'
        should_pause = False
        ai_action    = 'Campanha performando bem. Monitorar e escalar.'
    elif score >= 50:
        status_label = 'Regular'
        status_color = 'yellow'
        should_pause = False
        ai_action    = 'Campanha precisa de otimizacoes. Aplicar sugestoes antes de escalar.'
    elif score >= 30:
        status_label = 'Ruim'
        status_color = 'orange'
        should_pause = False
        ai_action    = 'Performance abaixo do aceitavel. Aplicar correcoes urgentes.'
    else:
        status_label = 'Critica'
        status_color = 'red'
        should_pause = True
        ai_action    = 'Recomendo pausar esta campanha — esta gerando prejuizo.'

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
    escalar = [r for r in results if r['score'] >= 75 and r.get('status') != 'paused']
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
