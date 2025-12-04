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
from scanner import run_scanner, add_custom_pool
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

class CustomPoolRequest(BaseModel):
    address: str
    pair: Optional[str] = None
    min_range: Optional[float] = None
    max_range: Optional[float] = None
    capital: Optional[float] = 1000

class FavoritePoolRequest(BaseModel):
    address: str
    is_custom: bool = False

# ========== FUNÇÕES AUXILIARES ==========

def safe_float(value, default=0.0):
    """Converte valor para float de forma segura"""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def safe_int(value, default=0):
    """Converte valor para int de forma segura"""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

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

# ========== POOLS CUSTOMIZADAS ENDPOINTS ==========

@app.post("/api/custom-pool")
async def add_custom_pool_endpoint(pool_data: CustomPoolRequest, background_tasks: BackgroundTasks):
    """Adiciona uma pool customizada para análise"""
    try:
        # Validar endereço
        if not pool_data.address or not pool_data.address.startswith('0x'):
            raise HTTPException(status_code=400, detail="Endereço inválido")
        
        # Adicionar pool customizada
        success = await add_custom_pool(supabase, pool_data.address.lower(), pool_data.pair)
        
        if success:
            # Marcar como favorita automaticamente
            favorite_data = {
                'pool_address': pool_data.address.lower(),
                'is_custom': True,
                'min_range': pool_data.min_range,
                'max_range': pool_data.max_range,
                'capital': pool_data.capital,
                'added_at': datetime.utcnow().isoformat()
            }
            
            # Verificar se já existe
            existing = supabase.table('favorite_pools').select('*').eq('pool_address', pool_data.address.lower()).execute()
            
            if not existing.data:
                supabase.table('favorite_pools').insert(favorite_data).execute()
            else:
                supabase.table('favorite_pools').update(favorite_data).eq('pool_address', pool_data.address.lower()).execute()
            
            # Analisar em background
            background_tasks.add_task(analyze_custom_pool_task, pool_data.address.lower())
            
            return {
                "status": "success",
                "message": f"Pool {pool_data.address} adicionada com sucesso",
                "address": pool_data.address.lower()
            }
        else:
            raise HTTPException(status_code=404, detail="Pool não encontrada ou erro ao adicionar")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao adicionar pool customizada: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def analyze_custom_pool_task(address: str):
    """Analisa pool customizada em background"""
    try:
        analyzer = PoolAnalyzer(supabase)
        await analyzer.analyze_pool(address)
        logger.info(f"✅ Análise completa para pool customizada {address}")
    except Exception as e:
        logger.error(f"❌ Erro ao analisar pool customizada {address}: {str(e)}")

@app.delete("/api/custom-pool/{address}")
async def remove_custom_pool(address: str):
    """Remove uma pool customizada"""
    try:
        # Remover dos favoritos
        supabase.table('favorite_pools').delete().eq('pool_address', address.lower()).execute()
        
        # Opcionalmente remover da tabela pools (ou apenas marcar como não-favorita)
        # supabase.table('pools').delete().eq('address', address.lower()).execute()
        
        return {
            "status": "success",
            "message": f"Pool {address} removida dos favoritos"
        }
        
    except Exception as e:
        logger.error(f"Erro ao remover pool: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/favorite-pools")
async def get_favorite_pools():
    """Retorna pools favoritas/customizadas"""
    try:
        # Buscar favoritos
        favorites = supabase.table('favorite_pools').select('*').execute()
        
        # Buscar detalhes das pools
        pools_data = []
        for fav in favorites.data:
            pool_result = supabase.table('pools').select('*').eq('address', fav['pool_address']).execute()
            if pool_result.data:
                pool = pool_result.data[0]
                pool['is_favorite'] = True
                pool['is_custom'] = fav.get('is_custom', False)
                pool['custom_min_range'] = fav.get('min_range')
                pool['custom_max_range'] = fav.get('max_range')
                pool['custom_capital'] = fav.get('capital')
                pools_data.append(pool)
        
        return {
            "status": "success",
            "count": len(pools_data),
            "pools": pools_data
        }
        
    except Exception as e:
        logger.error(f"Erro ao buscar favoritos: {str(e)}")
        return {"status": "success", "count": 0, "pools": []}

@app.post("/api/favorite-pool")
async def toggle_favorite(favorite_data: FavoritePoolRequest):
    """Adiciona/remove pool dos favoritos"""
    try:
        # Verificar se já é favorita
        existing = supabase.table('favorite_pools').select('*').eq('pool_address', favorite_data.address.lower()).execute()
        
        if existing.data:
            # Remover dos favoritos
            supabase.table('favorite_pools').delete().eq('pool_address', favorite_data.address.lower()).execute()
            return {"status": "success", "message": "Pool removida dos favoritos", "is_favorite": False}
        else:
            # Adicionar aos favoritos
            data = {
                'pool_address': favorite_data.address.lower(),
                'is_custom': favorite_data.is_custom,
                'added_at': datetime.utcnow().isoformat()
            }
            supabase.table('favorite_pools').insert(data).execute()
            return {"status": "success", "message": "Pool adicionada aos favoritos", "is_favorite": True}
            
    except Exception as e:
        logger.error(f"Erro ao alternar favorito: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== POOLS ENDPOINTS ==========

@app.get("/api/pools")
async def get_pools(
    limit: int = 50,
    min_score: Optional[int] = None,
    min_tvl: Optional[float] = None,
    include_favorites: bool = True
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
        
        # Marcar favoritos se solicitado
        if include_favorites:
            favorites = supabase.table('favorite_pools').select('pool_address, is_custom').execute()
            fav_addresses = {f['pool_address']: f['is_custom'] for f in favorites.data}
            
            for pool in result.data:
                pool['is_favorite'] = pool['address'] in fav_addresses
                pool['is_custom'] = fav_addresses.get(pool['address'], False)
        
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
        
        # Verificar se é favorita
        favorite = supabase.table('favorite_pools').select('*').eq('pool_address', address.lower()).execute()
        pool['is_favorite'] = len(favorite.data) > 0
        pool['is_custom'] = favorite.data[0].get('is_custom', False) if favorite.data else False
        
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
async def get_recommendations(limit: int = 10, include_custom: bool = True):
    """Retorna top pools recomendadas por score com destaque para customizadas"""
    try:
        # Buscar todas as pools com score >= 60
        result = supabase.table('pools')\
            .select('*')\
            .gte('score', 60)\
            .order('score', desc=True)\
            .limit(limit * 2)\
            .execute()
        
        pools = result.data
        
        # Buscar favoritos/customizadas
        favorites = supabase.table('favorite_pools').select('*').execute()
        fav_data = {f['pool_address']: f for f in favorites.data}
        
        # Separar pools customizadas e normais
        custom_pools = []
        regular_pools = []
        
        for pool in pools:
            # Parse ranges se existir
            ranges = {}
            if pool.get('ranges_data'):
                try:
                    ranges = json.loads(pool['ranges_data'])
                except:
                    pass
            
            # Melhor range baseado em simulações
            best_range = 'optimized'
            best_return = -999
            if pool.get('simulations_data'):
                try:
                    simulations = json.loads(pool['simulations_data'])
                    for strategy, sim in simulations.items():
                        ret = safe_float(sim.get('30d', {}).get('net_after_gas', -999))
                        if ret > best_return:
                            best_return = ret
                            best_range = strategy
                except:
                    pass
            
            rec = {
                "address": pool['address'],
                "pair": f"{pool['token0_symbol']}/{pool['token1_symbol']}",
                "score": safe_int(pool.get('score', 0)),
                "tvl_usd": safe_float(pool.get('tvl_usd', 0)),
                "volume_24h": safe_float(pool.get('volume_24h', 0)),
                "fee_apr": safe_float(pool.get('fee_apr', 0)),
                "il_7d": safe_float(pool.get('il_7d', 0)),
                "recommendation": pool.get('recommendation', ''),
                "best_range": best_range,
                "ranges": ranges,
                "explanation": pool.get('explanation', ''),
                "is_favorite": pool['address'] in fav_data,
                "is_custom": fav_data.get(pool['address'], {}).get('is_custom', False)
            }
            
            # Adicionar dados customizados se existirem
            if pool['address'] in fav_data:
                fav = fav_data[pool['address']]
                rec['custom_min_range'] = fav.get('min_range')
                rec['custom_max_range'] = fav.get('max_range')
                rec['custom_capital'] = fav.get('capital')
            
            # Separar customizadas das regulares
            if rec['is_custom']:
                custom_pools.append(rec)
            else:
                regular_pools.append(rec)
        
        # Combinar listas: customizadas primeiro
        all_recommendations = custom_pools + regular_pools
        
        return {
            "status": "success",
            "count": len(all_recommendations[:limit]),
            "custom_count": len(custom_pools),
            "recommendations": all_recommendations[:limit],
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
            # Tentar adicionar como pool customizada
            success = await add_custom_pool(supabase, address.lower())
            if not success:
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

# ========== DASHBOARD DATA ENDPOINT (CORRIGIDO) ==========

@app.get("/api/dashboard")
async def get_dashboard_data():
    """Retorna dados consolidados para o dashboard"""
    try:
        # Buscar estatísticas
        pools_result = supabase.table('pools').select('*').execute()
        positions_result = supabase.table('positions').select('*').eq('status', 'active').execute()
        
        pools = pools_result.data if pools_result.data else []
        positions = positions_result.data if positions_result.data else []
        
        # Calcular métricas com proteção contra None
        total_tvl = sum(safe_float(p.get('tvl_usd', 0)) for p in pools)
        total_volume = sum(safe_float(p.get('volume_24h', 0)) for p in pools)
        
        # Calcular APR médio com proteção
        aprs = [safe_float(p.get('fee_apr', 0)) for p in pools if p.get('fee_apr') is not None]
        avg_apr = sum(aprs) / len(aprs) if aprs else 0
        
        # Top performers
        scored_pools = [p for p in pools if p.get('score') is not None]
        top_pools = sorted(scored_pools, key=lambda x: safe_int(x.get('score', 0)), reverse=True)[:5]
        
        return {
            "status": "success",
            "metrics": {
                "total_pools": len(pools),
                "active_positions": len(positions),
                "total_tvl": total_tvl,
                "total_volume_24h": total_volume,
                "average_apr": avg_apr,
                "pools_analyzed": len([p for p in pools if safe_int(p.get('score', 0)) > 0])
            },
            "top_pools": [
                {
                    "pair": f"{p.get('token0_symbol', 'TOKEN0')}/{p.get('token1_symbol', 'TOKEN1')}",
                    "score": safe_int(p.get('score', 0)),
                    "tvl": safe_float(p.get('tvl_usd', 0)),
                    "apr": safe_float(p.get('fee_apr', 0))
                } for p in top_pools
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro ao buscar dados do dashboard: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
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
    
    # Criar tabela de favoritos se não existir
    try:
        # Verificar se tabela existe (tentativa de select)
        supabase.table('favorite_pools').select('pool_address').limit(1).execute()
    except:
        # Se der erro, a tabela não existe - você precisa criar no Supabase
        logger.warning("⚠️ Tabela 'favorite_pools' não existe. Crie no Supabase com campos: pool_address, is_custom, min_range, max_range, capital, added_at")
    
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
