import sys
with open('d:/pi-mining-1/backend/app/services/chat_service.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if 'def _extract_pi_evidence' in line:
        print(''.join(lines[i:i+40]))
        break
