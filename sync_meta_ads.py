"""
Sellvance - Meta Ads API Sync
Syncs campaigns and insights from Meta (Facebook/Instagram) Ads.
"""

import json
from database import get_db
from sync_base import get_valid_token, api_request, AuthError

GRAPH_API = "https://graph.facebook.com/v18.0"


def sync_all(org_id):
    """Run full Meta Ads sync. Returns total records synced."""
    token = get_valid_token(org_id, 'meta_ads')
    if not token:
        return 0

    total = 0

    # Get ad accounts
    try:
        data = api_request(
            f"{GRAPH_API}/me/adaccounts?fields=id,name,account_status",
            {"Authorization": f"Bearer {token}"}
        )
        accounts = data.get('data', [])
    except AuthError:
        raise
    except Exception as e:
        print(f"[meta_sync] Error fetching ad accounts: {e}")
        return 0

    for account in accounts:
        if account.get('account_status') != 1:  # 1 = ACTIVE
            continue
        account_id = account.get('id', '')
        total += _sync_campaigns(org_id, token, account_id)

    return total


def _sync_campaigns(org_id, token, account_id):
    """Fetch campaigns + insights for an ad account."""
    count = 0

    try:
        url = f"{GRAPH_API}/{account_id}/campaigns?fields=id,name,status,objective,daily_budget&limit=100"
        data = api_request(url, {"Authorization": f"Bearer {token}"})
        campaigns = data.get('data', [])
    except Exception as e:
        print(f"[meta_sync] Error fetching campaigns for {account_id}: {e}")
        return 0

    db = get_db()
    for camp in campaigns:
        camp_id = camp.get('id', '')
        name = camp.get('name', '')
        status = 'active' if camp.get('status') == 'ACTIVE' else 'paused'
        objective = camp.get('objective', 'CONVERSIONS').lower()
        daily_budget = (camp.get('daily_budget') or 0) / 100  # Meta returns in cents

        # Fetch insights
        spend = revenue = impressions = clicks = conversions = reach = 0
        try:
            insights_url = f"{GRAPH_API}/{camp_id}/insights?fields=spend,impressions,clicks,actions,action_values,reach,cpc,ctr&date_preset=last_30d"
            insights_data = api_request(insights_url, {"Authorization": f"Bearer {token}"})
            insights_list = insights_data.get('data', [])
            if insights_list:
                ins = insights_list[0]
                spend = float(ins.get('spend', 0))
                impressions = int(ins.get('impressions', 0))
                clicks = int(ins.get('clicks', 0))
                reach = int(ins.get('reach', 0))

                # Extract conversions and revenue from actions
                for action in (ins.get('actions') or []):
                    atype = action.get('action_type', '')
                    if 'purchase' in atype or 'offsite_conversion' in atype:
                        conversions += int(action.get('value', 0))

                for av in (ins.get('action_values') or []):
                    atype = av.get('action_type', '')
                    if 'purchase' in atype or 'offsite_conversion' in atype:
                        revenue += float(av.get('value', 0))
        except Exception as e:
            print(f"[meta_sync] Error fetching insights for campaign {camp_id}: {e}")

        # Map objective
        obj_map = {
            'conversions': 'conversao',
            'outcome_sales': 'conversao',
            'link_clicks': 'trafego',
            'outcome_traffic': 'trafego',
            'video_views': 'video',
            'outcome_awareness': 'video',
            'lead_generation': 'lead_gen',
            'outcome_leads': 'lead_gen',
        }
        mapped_obj = obj_map.get(objective, 'conversao')

        try:
            existing = db.execute(
                "SELECT id FROM ad_campaigns WHERE org_id=? AND external_campaign_id=?",
                (org_id, camp_id)
            ).fetchone()

            if existing:
                db.execute("""
                    UPDATE ad_campaigns SET name=?, status=?, objective=?, budget_daily=?,
                    spend=?, revenue=?, impressions=?, clicks=?, conversions=?, reach=?, date=date('now')
                    WHERE id=?
                """, (name, status, mapped_obj, daily_budget, spend, revenue, impressions, clicks, conversions, reach, existing['id']))
            else:
                db.execute("""
                    INSERT INTO ad_campaigns (org_id, platform, external_campaign_id, name, objective, spend, budget_daily, revenue, impressions, clicks, conversions, reach, status, date)
                    VALUES (?, 'meta', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, date('now'))
                """, (org_id, camp_id, name, mapped_obj, spend, daily_budget, revenue, impressions, clicks, conversions, reach, status))
            count += 1
        except Exception:
            pass

    db.commit()
    db.close()
    return count
