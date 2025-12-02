"""
EMITY System - API Principal
Liquidity Pool Intelligence System
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.requests import Request
from pydantic import BaseModel
from typing import Optional, List, Dict
import os
from datetime import datetime, timedelta
import asyncio
import json
import logging

# Import local modules
from database import get_supabase_client
from scanner import run_scanner
from analyzer import analyze_all_pools, PoolAnalyzer

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializar FastAPI
app = FastAPI(
    title="EMITY System",
    description="Liquidity Pool Intelligence - Sistema de Análise Institucional",
    version="1.0.0"
)

# Templates
templates = Jinja2Templates(directory="templates")

# Supabase client
supabase = get_supabase_client()

# ========== MODELOS ==========
class PoolResponse(BaseModel):
    address: str
    token0_symbol: str
    token1_symbol: str
    fee_tier: float
    tvl_usd: float
    volume_24h: float
    fee_apr: float
    score: int
    recommendation: Optional[str] = None

class RangeData(BaseModel):
    min_price: float
    max_price: float
    spread_percent: float
    strategy: str
    description: str

class SimulationData(BaseModel):
    time_in_range: float
    fees_collected: float
    impermanent_loss: float
    net_return: float
    gas_cost: float
    net_after_gas: float

# ========== ENDPOINTS PRINCIPAIS ==========

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Página principal com dashboard"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "EMITY System",
        "version": "1.0.0"
    }

# ========== SCANNER ENDPOINTS ==========

@app.get("/api/scan")
async def trigger_scan(background_tasks: BackgroundTasks):
    """Dispara scan manual de pools"""
    try:
        # Executar scan em background
        background_tasks.add_task(run_scan_task)
        
        return {
            "status": "success",
            "message": "Scan iniciado em background",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Erro ao iniciar scan: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def run_scan_task():
    """Task de scan em background"""
    try:
        logger.info("🚀 Iniciando scan task...")
        pools = await run_scanner(supabase)
        
        if pools:
            logger.info(f"✅ Scan completo: {len(pools)} pools encontradas")
            # Analisar pools após scan
            await analyze_all_pools(supabase)
        else:
            logger.warning("⚠️ Nenhuma pool encontrada no scan")
            
    except Exception as e:
        logger.error(f"❌ Erro na scan task: {str(e)}")

# ========== POOLS ENDPOINTS ==========

@app.get("/api/pools")
async def get_pools(
    limit: int = 50,
    min_score: Optional[int] = None,
    min_tvl: Optional[float] = None
):
    """Retorna lista de pools do banco"""
    try:
        query = supabase.table('pools').select('*')
        
        # Aplicar filtros
        if min_score:
            query = query.gte('score', min_score)
        if min_tvl:
            query = query.gte('tvl_usd', min_tvl)
        
        # Ordenar por score e limitar
        query = query.order('score', desc=True).limit(limit)
        
        result = query.execute()
        
        return {
            "status": "success",
            "count": len(result.data),
            "pools": result.data
        }
        
    except Exception as e:
        logger.error(f"Erro ao buscar pools: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pools/{address}")
async def get_pool_details(address: str):
    """Retorna detalhes completos de uma pool incluindo ranges"""
    try:
        # Buscar pool
        result = supabase.table('pools').select('*').eq('address', address.lower()).execute()
        
        if not result.data:
            raise HTTPException(status_code=404, detail="Pool não encontrada")
        
        pool = result.data[0]
        
        # Parse ranges e simulations se existirem
        if pool.get('ranges_data'):
            pool['ranges'] = json.loads(pool['ranges_data'])
        if pool.get('simulations_data'):
            pool['simulations'] = json.loads(pool['simulations_data'])
        
        return {
            "status": "success",
            "pool": pool
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar detalhes da pool: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== RECOMMENDATIONS ENDPOINTS ==========

@app.get("/api/recommendations")
async def get_recommendations(limit: int = 10):
    """Retorna top pools recomendadas por score"""
    try:
        # Buscar top pools por score
        result = supabase.table('pools')\
            .select('*')\
            .gte('score', 60)\
            .order('score', desc=True)\
            .limit(limit)\
            .execute()
        
        pools = result.data
        
        # Formatar para interface
        recommendations = []
        for pool in pools:
            # Parse ranges se existir
            ranges = {}
            if pool.get('ranges_data'):
                ranges = json.loads(pool['ranges_data'])
            
            # Melhor range baseado em simulações
            best_range = 'optimized'  # default
            if pool.get('simulations_data'):
                simulations = json.loads(pool['simulations_data'])
                # Encontrar melhor range por retorno 30d
                best_return = -999
                for strategy, sim in simulations.items():
                    if sim.get('30d', {}).get('net_after_gas', -999) > best_return:
                        best_return = sim['30d']['net_after_gas']
                        best_range = strategy
            
            rec = {
                "address": pool['address'],
                "pair": f"{pool['token0_symbol']}/{pool['token1_symbol']}",
                "score": pool['score'],
                "tvl_usd": pool['tvl_usd'],
                "volume_24h": pool['volume_24h'],
                "fee_apr": pool['fee_apr'],
                "il_7d": pool['il_7d'],
                "recommendation": pool.get('recommendation', ''),
                "best_range": best_range,
                "ranges": ranges,
                "explanation": pool.get('explanation', '')
            }
            recommendations.append(rec)
        
        return {
            "status": "success",
            "count": len(recommendations),
            "recommendations": recommendations,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro ao buscar recomendações: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== ACTIVE POSITIONS ENDPOINTS ==========

@app.get("/api/positions")
async def get_active_positions():
    """Retorna posições ativas do usuário"""
    try:
        result = supabase.table('positions')\
            .select('*')\
            .eq('status', 'active')\
            .execute()
        
        return {
            "status": "success",
            "count": len(result.data),
            "positions": result.data
        }
        
    except Exception as e:
        logger.error(f"Erro ao buscar posições: {str(e)}")
        return {"status": "success", "count": 0, "positions": []}

@app.post("/api/positions")
async def create_position(position: Dict):
    """Cria nova posição"""
    try:
        position['created_at'] = datetime.utcnow().isoformat()
        position['status'] = 'active'
        
        result = supabase.table('positions').insert(position).execute()
        
        return {
            "status": "success",
            "position": result.data[0]
        }
        
    except Exception as e:
        logger.error(f"Erro ao criar posição: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== ANÁLISE ENDPOINTS ==========

@app.post("/api/analyze/{address}")
async def analyze_pool(address: str, background_tasks: BackgroundTasks):
    """Dispara análise para uma pool específica"""
    try:
        # Verificar se pool existe
        result = supabase.table('pools').select('address').eq('address', address.lower()).execute()
        
        if not result.data:
            raise HTTPException(status_code=404, detail="Pool não encontrada")
        
        # Analisar em background
        background_tasks.add_task(analyze_single_pool, address.lower())
        
        return {
            "status": "success",
            "message": f"Análise iniciada para pool {address}",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao iniciar análise: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def analyze_single_pool(address: str):
    """Analisa uma pool específica"""
    try:
        analyzer = PoolAnalyzer(supabase)
        await analyzer.analyze_pool(address)
        logger.info(f"✅ Análise completa para pool {address}")
    except Exception as e:
        logger.error(f"❌ Erro ao analisar pool {address}: {str(e)}")

# ========== DASHBOARD DATA ENDPOINT ==========

@app.get("/api/dashboard")
async def get_dashboard_data():
    """Retorna dados consolidados para o dashboard"""
    try:
        # Buscar estatísticas
        pools_result = supabase.table('pools').select('*').execute()
        positions_result = supabase.table('positions').select('*').eq('status', 'active').execute()
        
        pools = pools_result.data
        positions = positions_result.data
        
        # Calcular métricas
        total_tvl = sum(p['tvl_usd'] for p in pools)
        total_volume = sum(p['volume_24h'] for p in pools)
        avg_apr = sum(p['fee_apr'] for p in pools) / len(pools) if pools else 0
        
        # Top performers
        top_pools = sorted(pools, key=lambda x: x['score'], reverse=True)[:5]
        
        return {
            "status": "success",
            "metrics": {
                "total_pools": len(pools),
                "active_positions": len(positions),
                "total_tvl": total_tvl,
                "total_volume_24h": total_volume,
                "average_apr": avg_apr,
                "pools_analyzed": len([p for p in pools if p.get('score', 0) > 0])
            },
            "top_pools": [
                {
                    "pair": f"{p['token0_symbol']}/{p['token1_symbol']}",
                    "score": p['score'],
                    "tvl": p['tvl_usd'],
                    "apr": p['fee_apr']
                } for p in top_pools
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro ao buscar dados do dashboard: {str(e)}")
        return {
            "status": "error",
            "metrics": {
                "total_pools": 0,
                "active_positions": 0,
                "total_tvl": 0,
                "total_volume_24h": 0,
                "average_apr": 0,
                "pools_analyzed": 0
            },
            "top_pools": [],
            "timestamp": datetime.utcnow().isoformat()
        }

# ========== STARTUP EVENTS ==========

@app.on_event("startup")
async def startup_event():
    """Executado ao iniciar a aplicação"""
    logger.info("🚀 EMITY System iniciando...")
    logger.info("📊 Dashboard: https://emity-system.onrender.com")
    logger.info("📡 API Docs: https://emity-system.onrender.com/docs")
    
    # Agendar primeiro scan em 1 minuto
    asyncio.create_task(initial_scan())

async def initial_scan():
    """Scan inicial após startup"""
    await asyncio.sleep(60)  # Aguardar 1 minuto
    logger.info("🔄 Executando scan inicial...")
    await run_scan_task()

# ========== SCHEDULED TASKS (para Worker) ==========

async def scheduled_scanner():
    """Scanner agendado para rodar periodicamente"""
    while True:
        try:
            logger.info("⏰ Executando scan agendado...")
            await run_scan_task()
            await asyncio.sleep(1800)  # 30 minutos
        except Exception as e:
            logger.error(f"Erro no scanner agendado: {str(e)}")
            await asyncio.sleep(300)  # Retry em 5 minutos

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
