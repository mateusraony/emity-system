"""
EMITY - Liquidity Pool Intelligence System
Main FastAPI Application
"""

import os
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv
from datetime import datetime
from database import db

# Carrega variáveis de ambiente
load_dotenv()

# Inicializa FastAPI
app = FastAPI(
    title="EMITY - Liquidity Pool Intelligence",
    description="Sistema institucional de análise e recomendação de pools DeFi",
    version="1.0.0"
)

# Configura templates
templates = Jinja2Templates(directory="templates")

# ==================== ENDPOINTS BÁSICOS ====================

@app.get("/health")
async def health_check():
    """Endpoint de health check para o Render"""
    return {
        "status": "online",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
        "system": "EMITY",
        "modules": {
            "api": "ready",
            "scanner": "preparing",
            "analyzer": "preparing",
            "telegram": "preparing"
        }
    }

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Página principal - Interface Web"""
    # Por enquanto, retorna o HTML do preview
    # Você deve colar o conteúdo do emity_preview_v3.html no arquivo templates/index.html
    return templates.TemplateResponse("index.html", {"request": request})

# ==================== ENDPOINTS DA API ====================

@app.get("/api/pools")
async def get_pools():
    """Retorna todas as pools analisadas"""
    pools = await db.get_all_pools()
    return {
        "success": True,
        "count": len(pools),
        "pools": pools,
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/pools/{pool_address}")
async def get_pool_detail(pool_address: str):
    """Retorna detalhes de uma pool específica"""
    # TODO: Implementar na Fase 1
    return {
        "success": True,
        "pool_address": pool_address,
        "message": "Endpoint será implementado na Fase 1"
    }

@app.get("/api/recommendations")
async def get_recommendations():
    """Retorna pools recomendadas baseadas no perfil"""
    # TODO: Implementar na Fase 1
    return {
        "success": True,
        "recommendations": [],
        "message": "Scanner será ativado na Fase 1"
    }

@app.post("/api/config")
async def update_config(config: dict):
    """Atualiza configurações do usuário"""
    try:
        for key, value in config.items():
            await db.set_config(key, str(value))
        return {
            "success": True,
            "message": "Configurações atualizadas"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@app.get("/api/alerts")
async def get_alerts():
    """Retorna alertas recentes"""
    # TODO: Implementar na Fase 1
    return {
        "success": True,
        "alerts": [],
        "message": "Sistema de alertas será ativado na Fase 3"
    }

@app.get("/api/stats")
async def get_stats():
    """Retorna estatísticas do sistema"""
    return {
        "success": True,
        "stats": {
            "pools_tracked": 0,
            "active_positions": 0,
            "total_pnl": 0,
            "alerts_today": 0
        },
        "timestamp": datetime.now().isoformat()
    }

# ==================== STARTUP ====================

@app.on_event("startup")
async def startup_event():
    """Inicialização do sistema"""
    print("=" * 60)
    print("EMITY - Liquidity Pool Intelligence")
    print("Sistema iniciando...")
    print("=" * 60)
    
    # Testa conexão com banco
    try:
        config_test = await db.get_config("system_started")
        await db.set_config("system_started", datetime.now().isoformat())
        print("✅ Banco de dados conectado")
    except Exception as e:
        print(f"⚠️ Erro ao conectar banco: {e}")
    
    print("✅ API online em /health")
    print("✅ Interface web disponível em /")
    print("=" * 60)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
