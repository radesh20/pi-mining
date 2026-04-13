import sys
from app.services.celonis_service import CelonisService

try:
    c = CelonisService()
    pql = 'COUNT(TABLE("t_o_custom_VimHeader"))'
    res = c._run_pql(pql, 'count')
    print('Total rows in t_o_custom_VimHeader:', res.iloc[0, 0])
except Exception as e:
    print('Failed:', str(e))
