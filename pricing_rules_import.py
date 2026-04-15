# pricing_rules_import.py — Importacao de regras de pricing via CSV
# Permite ao usuario subir um CSV com SKUs + min/max/margem e ativar
# a IA de pricing de uma vez em todos os produtos.
#
# Formato CSV esperado:
#   sku,min_price,max_price,cost_price,target_margin,strategy,auto_apply
#   SKU001,49.90,89.90,30.00,0.25,buybox,1
#   SKU002,99.00,149.00,60.00,0.30,match,0

import csv
import io
from database import get_db


VALID_STRATEGIES = {'buybox', 'match', 'premium', 'aggressive'}


def parse_csv(csv_content):
    """
    Parse o conteudo CSV e retorna lista de regras validadas.
    """
    reader = csv.DictReader(io.StringIO(csv_content))
    rules = []
    errors = []
    row_num = 1

    for row in reader:
        row_num += 1
        try:
            sku = (row.get('sku') or '').strip()
            if not sku:
                errors.append(f'Linha {row_num}: sku vazio')
                continue

            min_price = float(row.get('min_price', 0) or 0)
            max_price = float(row.get('max_price', 0) or 0)
            cost_price = float(row.get('cost_price', 0) or 0)
            target_margin = float(row.get('target_margin', 0.20) or 0.20)
            strategy = (row.get('strategy') or 'buybox').strip().lower()
            auto_apply = int(row.get('auto_apply', 0) or 0)
            marketplace = (row.get('marketplace') or 'all').strip().lower()

            # Validacoes
            if min_price <= 0:
                errors.append(f'Linha {row_num} ({sku}): min_price deve ser > 0')
                continue
            if max_price <= min_price:
                errors.append(f'Linha {row_num} ({sku}): max_price deve ser > min_price')
                continue
            if cost_price <= 0:
                errors.append(f'Linha {row_num} ({sku}): cost_price deve ser > 0')
                continue
            if cost_price >= min_price:
                errors.append(f'Linha {row_num} ({sku}): cost_price deve ser < min_price')
                continue
            if strategy not in VALID_STRATEGIES:
                errors.append(f'Linha {row_num} ({sku}): strategy invalida (use: {", ".join(VALID_STRATEGIES)})')
                continue

            rules.append({
                'sku': sku,
                'min_price': min_price,
                'max_price': max_price,
                'cost_price': cost_price,
                'target_margin': target_margin,
                'strategy': strategy,
                'auto_apply': auto_apply,
                'marketplace': marketplace,
            })
        except (ValueError, TypeError) as e:
            errors.append(f'Linha {row_num}: formato invalido ({e})')
            continue

    return rules, errors


def import_rules(org_id, csv_content, mode='upsert'):
    """
    Importa regras do CSV.
    mode: 'upsert' (atualiza existentes) ou 'replace' (apaga todas antes)
    """
    rules, errors = parse_csv(csv_content)

    db = get_db()
    if mode == 'replace':
        db.execute('DELETE FROM pricing_rules WHERE org_id=?', (org_id,))

    inserted = 0
    updated = 0
    for r in rules:
        existing = db.execute(
            'SELECT id FROM pricing_rules WHERE org_id=? AND sku=? AND marketplace=?',
            (org_id, r['sku'], r['marketplace'])
        ).fetchone()

        if existing:
            db.execute(
                '''UPDATE pricing_rules
                   SET min_price=?, max_price=?, cost_price=?, target_margin=?,
                       strategy=?, auto_apply=?, is_active=1, updated_at=datetime('now')
                   WHERE id=?''',
                (r['min_price'], r['max_price'], r['cost_price'],
                 r['target_margin'], r['strategy'], r['auto_apply'], existing['id'])
            )
            updated += 1
        else:
            db.execute(
                '''INSERT INTO pricing_rules
                   (org_id, sku, marketplace, min_price, max_price, cost_price,
                    target_margin, strategy, auto_apply, is_active)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)''',
                (org_id, r['sku'], r['marketplace'], r['min_price'], r['max_price'],
                 r['cost_price'], r['target_margin'], r['strategy'], r['auto_apply'])
            )
            inserted += 1

    db.commit()
    db.close()

    try:
        from telemetry import track
        track('action', 'pricing_rules_imported',
              inserted=inserted, updated=updated, errors=len(errors))
    except Exception:
        pass

    return {
        'ok': True,
        'inserted': inserted,
        'updated': updated,
        'errors': errors,
        'total_valid': len(rules),
    }


def export_rules(org_id):
    """Exporta regras atuais em CSV."""
    db = get_db()
    rows = db.execute(
        '''SELECT sku, marketplace, min_price, max_price, cost_price,
                  target_margin, strategy, auto_apply
           FROM pricing_rules
           WHERE org_id=? AND is_active=1
           ORDER BY sku''',
        (org_id,)
    ).fetchall()
    db.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'sku', 'marketplace', 'min_price', 'max_price', 'cost_price',
        'target_margin', 'strategy', 'auto_apply'
    ])
    for r in rows:
        writer.writerow([
            r['sku'], r['marketplace'], r['min_price'], r['max_price'],
            r['cost_price'], r['target_margin'], r['strategy'], r['auto_apply']
        ])
    return output.getvalue()


def sample_csv():
    """Retorna um CSV de exemplo para download."""
    return '''sku,marketplace,min_price,max_price,cost_price,target_margin,strategy,auto_apply
SKU001,mercado_livre,49.90,89.90,30.00,0.25,buybox,1
SKU002,mercado_livre,99.00,149.00,60.00,0.30,match,0
SKU003,amazon,25.50,45.00,15.00,0.30,buybox,1
SKU004,shopee,19.90,39.90,10.00,0.40,aggressive,1
'''
