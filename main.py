"""EMITY System - API Principal
Fase 3: Liquidity Pool Intelligence + Motor de Risco + Automação/Telegram
"""

import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.requests import Request
from pydantic import BaseModel, Field

from database import EMITYDatabase
from scanner import run_scanner, add_custom_pool
from analyzer import analyze_all_pools, PoolAnalyzer
from risk_engine import RiskEngine
from telegram_bot import telegram_bot

# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ============================================================
# App / CORS / Templates
# ============================================================

app = FastAPI(
    title="EMITY System - Liquidity Pool Intelligence",
    description="Sistema institucional de análise de pools + motor de risco + automação",
    version="3.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ajuste depois se quiser limitar
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory="templates")

# ============================================================
# Supabase / DB
# ============================================================

try:
    db = EMITYDatabase()
    supabase = db.client  # reaproveita o client interno
    logger.info("✅ EMITYDatabase e Supabase inicializados")
except Exception as e:
    logger.error(f"❌ Erro ao inicializar EMITYDatabase: {e}")
    db = None
    supabase = None

# ============================================================
# Pydantic Models
# ============================================================

# --- Modelos da Fase 1 (mantidos) ---

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


# Novo modelo para adicionar favoritos (compatível com frontend)
class FavoriteAddRequest(BaseModel):
    pool_address: str
    pool_name: Optional[str] = None
    notes: Optional[str] = None
    performance_score: Optional[float] = None


# --- Modelos da Fase 2 (config / risco) ---

class ConfigUpdate(BaseModel):
    """Payload para atualizar a configuração de risco do usuário"""
    capital_total: Optional[float] = Field(None, ge=100, le=10_000_000)
    perfil_risco: Optional[str] = Field(
        None, pattern="^(conservador|moderado|agressivo)$"
    )
    max_positions: Optional[int] = Field(None, ge=1, le=20)
    stop_loss: Optional[float] = Field(None, ge=1, le=50)
    max_position_size: Optional[float] = Field(None, ge=5, le=100)
    min_score: Optional[int] = Field(None, ge=0, le=100)
    gas_multiplier: Optional[float] = Field(None, ge=1, le=10)


class PositionSizeRequest(BaseModel):
    pool_address: str
    override_pct: Optional[float] = None


# --- Modelos da Fase 3 (Telegram) ---

class TelegramConfig(BaseModel):
    """Configuração do bot Telegram"""
    enabled: bool
    bot_token: Optional[str] = None
    chat_id: Optional[str] = None


class AlertTest(BaseModel):
    """Teste de alerta"""
    message: Optional[str] = None
    alert_type: str = "test"


# ============================================================
# Helpers
# ============================================================

def safe_float(value, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def build_simulation_7d_from_pool(pool: Dict) -> Optional[Dict]:
    """Extrai um bloco compacto simulation_7d a partir de simulations_data da pool.

    - Usa o melhor range com base em net_after_gas (7d)
    - Converte net_return/net_after_gas (que são %) em retorno estimado em USD
      assumindo capital de 1000 USDT (consistente com o analyzer)
    """
    sims_raw = pool.get("simulations_data") or pool.get("simulations")  # pode já estar carregado
    if not sims_raw:
        return None

    try:
        simulations = sims_raw
        if isinstance(sims_raw, str):
            simulations = json.loads(sims_raw)
    except Exception:
        logger.warning(
            "Não foi possível parsear simulations_data para pool %s",
            pool.get("pool_address") or pool.get("address"),
        )
        return None

    best = None
    best_net_after_gas_7d = -1e9

    for strategy, data in simulations.items():
        period_7d = data.get("7d") or {}
        net_after_gas = safe_float(period_7d.get("net_after_gas", 0))
        if net_after_gas > best_net_after_gas_7d:
            best_net_after_gas_7d = net_after_gas
            best = period_7d

    if not best:
        return None

    # best contém campos em %, baseando-se em capital de 1000
    net_return_pct = safe_float(best.get("net_after_gas", best.get("net_return", 0)))
    il_pct = safe_float(best.get("impermanent_loss", 0))

    # converter % para retorno estimado em USDT assumindo 1000
    capital_ref = 1000.0
    net_return_usd = capital_ref * (net_return_pct / 100.0)

    return {
        "net_return": net_return_usd,
        "il_percentage": il_pct,
        "raw": {
            "net_after_gas_pct": net_return_pct,
            "impermanent_loss_pct": il_pct,
        },
    }


def attach_simulation_7d(pool: Dict) -> Dict:
    """Garante que a pool tenha o campo simulation_7d esperado pelo RiskEngine."""
    sim_7d = build_simulation_7d_from_pool(pool)
    if sim_7d:
        pool["simulation_7d"] = sim_7d
    return pool


# ============================================================
# ROOT / HEALTH
# ============================================================

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Página principal do dashboard (templates/index.html)"""
    try:
        return templates.TemplateResponse("index.html", {"request": request})
    except Exception:
        return HTMLResponse(
            content="<h1>EMITY System</h1><p>Dashboard em deploy...</p>",
        )


@app.get("/health")
async def health_check():
    """Health check unificado (infra + componentes)"""
    components_status = {
        "database": db is not None,
        "supabase_client": supabase is not None,
        "telegram_bot": telegram_bot.enabled
    }

    all_healthy = all(components_status.values())

    return {
        "status": "healthy" if all_healthy else "degraded",
        "components": components_status,
        "service": "EMITY System",
        "version": app.version,
        "timestamp": datetime.utcnow().isoformat(),
    }


# ============================================================
# TELEGRAM / ALERTAS (Fase 3 - NOVO)
# ============================================================

@app.post("/api/telegram/enable")
async def enable_telegram(config: TelegramConfig):
    """Ativa/desativa bot do Telegram"""
    try:
        # Atualizar configuração
        telegram_bot.enabled = config.enabled
        
        if config.bot_token:
            telegram_bot.token = config.bot_token
            telegram_bot.base_url = f"https://api.telegram.org/bot{config.bot_token}"
            
        if config.chat_id:
            telegram_bot.chat_id = config.chat_id
            
        # Salvar no banco
        if supabase:
            supabase.table("config").upsert({
                "key": "telegram_enabled",
                "value": str(config.enabled),
                "updated_at": datetime.utcnow().isoformat()
            }).execute()
            
        return {
            "success": True,
            "enabled": telegram_bot.enabled,
            "message": f"Telegram {'ativado' if telegram_bot.enabled else 'desativado'}"
        }
        
    except Exception as e:
        logger.error(f"Erro ao configurar Telegram: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/telegram/status")
async def telegram_status():
    """Retorna status do bot Telegram"""
    try:
        # Contar alertas ativos
        alerts_count = 0
        if supabase:
            result = supabase.table("alerts").select("id").execute()
            alerts_count = len(result.data or [])
            
        return {
            "enabled": telegram_bot.enabled,
            "bot_token_configured": bool(telegram_bot.token),
            "chat_id_configured": bool(telegram_bot.chat_id),
            "dashboard_url": telegram_bot.dashboard_url,
            "total_alerts_sent": alerts_count,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro ao buscar status: {e}")
        return {
            "enabled": False,
            "error": str(e)
        }


@app.post("/api/telegram/test")
async def test_telegram_alert(test_data: AlertTest):
    """Envia alerta de teste para Telegram"""
    try:
        if not telegram_bot.enabled:
            raise HTTPException(status_code=400, detail="Telegram desabilitado")
            
        # Enviar mensagem de teste
        success = await telegram_bot.send_test_message()
        
        if success:
            # Registrar no banco
            if supabase:
                supabase.table("alerts").insert({
                    "type": "TEST",
                    "title": "Teste de Alerta",
                    "message": test_data.message or "Teste executado com sucesso",
                    "severity": "info",
                    "created_at": datetime.utcnow().isoformat()
                }).execute()
                
            return {
                "success": True,
                "message": "Alerta de teste enviado com sucesso"
            }
        else:
            raise HTTPException(status_code=500, detail="Falha ao enviar alerta")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao enviar teste: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/alerts/history")
async def get_alerts_history(
    limit: int = Query(50, ge=1, le=200),
    alert_type: Optional[str] = None
):
    """Retorna histórico de alertas enviados"""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")
        
    try:
        query = supabase.table("alerts").select("*")
        
        if alert_type:
            query = query.eq("type", alert_type)
            
        query = query.order("created_at", desc=True).limit(limit)
        result = query.execute()
        
        alerts = result.data or []
        
        # Agrupar por tipo
        alerts_by_type = {}
        for alert in alerts:
            alert_type = alert.get("type", "UNKNOWN")
            if alert_type not in alerts_by_type:
                alerts_by_type[alert_type] = []
            alerts_by_type[alert_type].append(alert)
            
        return {
            "success": True,
            "total": len(alerts),
            "alerts": alerts,
            "by_type": alerts_by_type,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Erro ao buscar histórico: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/alerts/clear")
async def clear_alerts_history():
    """Limpa histórico de alertas antigos (mantém últimos 7 dias)"""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")
        
    try:
        # Deletar alertas com mais de 7 dias
        cutoff_date = (datetime.utcnow() - timedelta(days=7)).isoformat()
        
        result = supabase.table("alerts").delete().lt("created_at", cutoff_date).execute()
        
        return {
            "success": True,
            "deleted": len(result.data or []),
            "message": "Histórico de alertas limpo (mantidos últimos 7 dias)"
        }
        
    except Exception as e:
        logger.error(f"Erro ao limpar histórico: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# SCANNER (Fase 1)
# ============================================================

async def run_scan_task():
    """Task de scan em background (usa scanner.run_scanner + analyze_all_pools)."""
    if not supabase:
        logger.error("run_scan_task chamado sem supabase client")
        return

    try:
        logger.info("🚀 Iniciando scan task...")
        pools = await run_scanner(supabase)

        if pools:
            logger.info("✅ Scan completo: %d pools encontradas", len(pools))
            await analyze_all_pools(supabase)
        else:
            logger.warning("⚠️ Nenhuma pool encontrada no scan")

    except Exception as e:
        logger.error(f"❌ Erro na scan task: {e}")


@app.get("/api/scan")
async def trigger_scan(background_tasks: BackgroundTasks):
    """Dispara scan manual de pools (em background)."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        background_tasks.add_task(run_scan_task)
        return {
            "status": "success",
            "message": "Scan iniciado em background",
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        logger.error(f"Erro ao iniciar scan: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# POOLS CUSTOM / FAVORITOS (Fase 1 + NOVOS ENDPOINTS)
# ============================================================

@app.post("/api/custom-pool")
async def add_custom_pool_endpoint(
    pool_data: CustomPoolRequest, background_tasks: BackgroundTasks
):
    """Adiciona uma pool customizada para análise e marca como favorita."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        if not pool_data.address or not pool_data.address.startswith("0x"):
            raise HTTPException(status_code=400, detail="Endereço inválido")

        address = pool_data.address.lower()

        success = await add_custom_pool(supabase, address, pool_data.pair)

        if not success:
            raise HTTPException(
                status_code=404, detail="Pool não encontrada ou erro ao adicionar"
            )

        favorite_data = {
            "pool_address": address,
            "is_custom": True,
            "min_range": pool_data.min_range,
            "max_range": pool_data.max_range,
            "capital": pool_data.capital,
            "added_at": datetime.utcnow().isoformat(),
        }

        existing = (
            supabase.table("favorite_pools")
            .select("*")
            .eq("pool_address", address)
            .execute()
        )

        if not existing.data:
            supabase.table("favorite_pools").insert(favorite_data).execute()
        else:
            supabase.table("favorite_pools").update(favorite_data).eq(
                "pool_address", address
            ).execute()

        # analisar em background
        background_tasks.add_task(analyze_custom_pool_task, address)

        return {
            "status": "success",
            "message": f"Pool {address} adicionada com sucesso",
            "address": address,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao adicionar pool customizada: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def analyze_custom_pool_task(address: str):
    """Analisa pool customizada em background (usa PoolAnalyzer da Fase 1)."""
    if not supabase:
        logger.error("analyze_custom_pool_task chamado sem supabase client")
        return

    try:
        analyzer = PoolAnalyzer(supabase)
        await analyzer.analyze_pool(address)
        logger.info("✅ Análise completa para pool customizada %s", address)
    except Exception as e:
        logger.error(f"❌ Erro ao analisar pool customizada {address}: {e}")


@app.delete("/api/custom-pool/{address}")
async def remove_custom_pool(address: str):
    """Remove uma pool customizada dos favoritos."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        supabase.table("favorite_pools").delete().eq(
            "pool_address", address.lower()
        ).execute()

        return {
            "status": "success",
            "message": f"Pool {address} removida dos favoritos",
        }
    except Exception as e:
        logger.error(f"Erro ao remover pool: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/favorite-pools")
async def get_favorite_pools():
    """Retorna pools favoritas/customizadas com detalhes."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        favorites = supabase.table("favorite_pools").select("*").execute()

        pools_data: List[Dict] = []
        for fav in favorites.data or []:
            pool_result = (
                supabase.table("pools")
                .select("*")
                .eq("address", fav["pool_address"])
                .execute()
            )
            if pool_result.data:
                pool = pool_result.data[0]
                pool["is_favorite"] = True
                pool["is_custom"] = fav.get("is_custom", False)
                pool["custom_min_range"] = fav.get("min_range")
                pool["custom_max_range"] = fav.get("max_range")
                pool["custom_capital"] = fav.get("capital")
                pools_data.append(pool)

        return {
            "status": "success",
            "count": len(pools_data),
            "pools": pools_data,
        }

    except Exception as e:
        logger.error(f"Erro ao buscar favoritos: {e}")
        return {"status": "success", "count": 0, "pools": []}


@app.post("/api/favorite-pool")
async def toggle_favorite(favorite_data: FavoritePoolRequest):
    """Adiciona/remove pool dos favoritos (tabela favorite_pools)."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        address = favorite_data.address.lower()

        existing = (
            supabase.table("favorite_pools")
            .select("*")
            .eq("pool_address", address)
            .execute()
        )

        if existing.data:
            supabase.table("favorite_pools").delete().eq(
                "pool_address", address
            ).execute()
            return {
                "status": "success",
                "message": "Pool removida dos favoritos",
                "is_favorite": False,
            }
        else:
            data = {
                "pool_address": address,
                "is_custom": favorite_data.is_custom,
                "added_at": datetime.utcnow().isoformat(),
            }
            supabase.table("favorite_pools").insert(data).execute()
            return {
                "status": "success",
                "message": "Pool adicionada aos favoritos",
                "is_favorite": True,
            }

    except Exception as e:
        logger.error(f"Erro ao alternar favorito: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# NOVOS ENDPOINTS DE FAVORITOS (FASE 2 - FINALIZANDO)
# ============================================================

@app.post("/api/favorites/add")
async def add_to_favorites(favorite_data: FavoriteAddRequest):
    """Adiciona pool aos favoritos (endpoint novo para compatibilidade com frontend)."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        address = favorite_data.pool_address.lower()
        
        # Verificar se já existe
        existing = (
            supabase.table("favorite_pools")
            .select("*")
            .eq("pool_address", address)
            .execute()
        )
        
        data = {
            "pool_address": address,
            "pool_name": favorite_data.pool_name,
            "user_id": "default_user",  # para multi-usuário no futuro
            "network": "arbitrum",
            "notes": favorite_data.notes or "",
            "performance_score": favorite_data.performance_score,
            "added_at": datetime.utcnow().isoformat(),
            "last_checked": datetime.utcnow().isoformat(),
            "is_active": True,
            "metadata": json.dumps({
                "source": "web_interface",
                "version": "2.0"
            })
        }
        
        if existing.data:
            # Atualizar existente
            result = supabase.table("favorite_pools").update(data).eq(
                "pool_address", address
            ).execute()
        else:
            # Inserir novo
            result = supabase.table("favorite_pools").insert(data).execute()
        
        return {
            "success": True,
            "message": f"Pool {favorite_data.pool_name or address} adicionada aos favoritos",
            "data": result.data[0] if result.data else None
        }
        
    except Exception as e:
        logger.error(f"Erro ao adicionar favorito: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/favorites/remove/{pool_address}")
async def remove_from_favorites(pool_address: str):
    """Remove pool dos favoritos (endpoint novo para compatibilidade com frontend)."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        address = pool_address.lower()
        
        result = supabase.table("favorite_pools").delete().eq(
            "pool_address", address
        ).execute()
        
        if result.data:
            return {
                "success": True,
                "message": f"Pool {address} removida dos favoritos"
            }
        else:
            return {
                "success": False,
                "message": "Pool não encontrada nos favoritos"
            }
            
    except Exception as e:
        logger.error(f"Erro ao remover favorito: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/favorites/list")
async def list_favorites():
    """Lista todas as pools favoritas com detalhes completos."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        # Buscar favoritos
        favorites = supabase.table("favorite_pools").select("*").execute()
        
        if not favorites.data:
            return {
                "success": True,
                "count": 0,
                "favorites": []
            }
        
        # Enriquecer com dados das pools
        enriched_favorites = []
        for fav in favorites.data:
            # Buscar dados da pool
            pool_result = (
                supabase.table("pools")
                .select("*")
                .eq("address", fav["pool_address"])
                .execute()
            )
            
            if pool_result.data:
                pool = pool_result.data[0]
                # Combinar dados
                enriched_fav = {
                    **fav,
                    "pool_details": {
                        "pair": f"{pool.get('token0_symbol', 'TOKEN0')}/{pool.get('token1_symbol', 'TOKEN1')}",
                        "fee_tier": pool.get("fee_tier", 0),
                        "tvl_usd": pool.get("tvl_usd", 0),
                        "volume_24h": pool.get("volume_24h", 0),
                        "fee_apr": pool.get("fee_apr", 0),
                        "score": pool.get("score", 0),
                        "il_7d": pool.get("il_7d", 0),
                        "recommendation": pool.get("recommendation", ""),
                    }
                }
            else:
                # Pool não encontrada na tabela principal
                enriched_fav = {
                    **fav,
                    "pool_details": None
                }
            
            enriched_favorites.append(enriched_fav)
        
        return {
            "success": True,
            "count": len(enriched_favorites),
            "favorites": enriched_favorites
        }
        
    except Exception as e:
        logger.error(f"Erro ao listar favoritos: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# POOLS / RECOMENDAÇÕES / POSIÇÕES (Fase 1)
# ============================================================

@app.get("/api/pools")
async def get_pools(
    limit: int = 50,
    min_score: Optional[int] = None,
    min_tvl: Optional[float] = None,
    include_favorites: bool = True,
):
    """Retorna lista de pools do banco (com filtros)."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        query = supabase.table("pools").select("*")

        if min_score is not None:
            query = query.gte("score", min_score)
        if min_tvl is not None:
            query = query.gte("tvl_usd", min_tvl)

        query = query.order("score", desc=True).limit(limit)
        result = query.execute()
        pools = result.data or []

        if include_favorites:
            favorites = supabase.table("favorite_pools").select(
                "pool_address, is_custom"
            ).execute()
            fav_addresses = {f["pool_address"]: f["is_custom"] for f in favorites.data or []}

            for pool in pools:
                addr = pool.get("address") or pool.get("pool_address")
                pool["is_favorite"] = addr in fav_addresses
                pool["is_custom"] = fav_addresses.get(addr, False)

        return {"status": "success", "count": len(pools), "pools": pools}

    except Exception as e:
        logger.error(f"Erro ao buscar pools: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pools/{address}")
async def get_pool_details(address: str):
    """Retorna detalhes completos de uma pool incluindo ranges/simulações."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        result = (
            supabase.table("pools")
            .select("*")
            .eq("address", address.lower())
            .execute()
        )

        if not result.data:
            raise HTTPException(status_code=404, detail="Pool não encontrada")

        pool = result.data[0]

        if pool.get("ranges_data"):
            try:
                pool["ranges"] = json.loads(pool["ranges_data"])
            except Exception:
                pool["ranges"] = {}

        if pool.get("simulations_data"):
            try:
                pool["simulations"] = json.loads(pool["simulations_data"])
            except Exception:
                pool["simulations"] = {}

        favorite = (
            supabase.table("favorite_pools")
            .select("*")
            .eq("pool_address", address.lower())
            .execute()
        )
        pool["is_favorite"] = bool(favorite.data)
        pool["is_custom"] = (
            favorite.data[0].get("is_custom", False) if favorite.data else False
        )

        return {"status": "success", "pool": pool}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar detalhes da pool: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/recommendations")
async def get_recommendations(limit: int = 10, include_custom: bool = True):
    """Retorna top pools recomendadas por score com destaque para customizadas."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        result = (
            supabase.table("pools")
            .select("*")
            .gte("score", 60)
            .order("score", desc=True)
            .limit(limit * 2)
            .execute()
        )

        pools = result.data or []

        favorites = supabase.table("favorite_pools").select("*").execute()
        fav_data = {f["pool_address"]: f for f in favorites.data or []}

        custom_pools: List[Dict] = []
        regular_pools: List[Dict] = []

        for pool in pools:
            ranges = {}
            if pool.get("ranges_data"):
                try:
                    ranges = json.loads(pool["ranges_data"])
                except Exception:
                    ranges = {}

            best_range = "optimized"
            best_return = -999.0
            if pool.get("simulations_data"):
                try:
                    simulations = json.loads(pool["simulations_data"])
                    for strategy, sim in simulations.items():
                        ret = safe_float(sim.get("30d", {}).get("net_after_gas", -999))
                        if ret > best_return:
                            best_return = ret
                            best_range = strategy
                except Exception:
                    pass

            addr = pool.get("address") or pool.get("pool_address")
            rec = {
                "address": addr,
                "pair": f"{pool.get('token0_symbol', 'TOKEN0')}/{pool.get('token1_symbol', 'TOKEN1')}",
                "score": safe_int(pool.get("score", 0)),
                "tvl_usd": safe_float(pool.get("tvl_usd", 0)),
                "volume_24h": safe_float(pool.get("volume_24h", 0)),
                "fee_apr": safe_float(pool.get("fee_apr", 0)),
                "il_7d": safe_float(pool.get("il_7d", 0)),
                "recommendation": pool.get("recommendation", ""),
                "best_range": best_range,
                "ranges": ranges,
                "explanation": pool.get("explanation", ""),
                "is_favorite": addr in fav_data,
                "is_custom": fav_data.get(addr, {}).get("is_custom", False),
            }

            if addr in fav_data:
                fav = fav_data[addr]
                rec["custom_min_range"] = fav.get("min_range")
                rec["custom_max_range"] = fav.get("max_range")
                rec["custom_capital"] = fav.get("capital")

            if rec["is_custom"]:
                custom_pools.append(rec)
            else:
                regular_pools.append(rec)

        all_recommendations = custom_pools + regular_pools

        return {
            "status": "success",
            "count": len(all_recommendations[:limit]),
            "custom_count": len(custom_pools),
            "recommendations": all_recommendations[:limit],
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Erro ao buscar recomendações: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/positions")
async def get_active_positions():
    """Retorna posições ativas da tabela positions (se existir)."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        result = (
            supabase.table("positions")
            .select("*")
            .eq("status", "active")
            .execute()
        )

        return {
            "status": "success",
            "count": len(result.data or []),
            "positions": result.data or [],
        }

    except Exception as e:
        logger.error(f"Erro ao buscar posições: {e}")
        return {"status": "success", "count": 0, "positions": []}


@app.post("/api/positions")
async def create_position(position: Dict):
    """Cria nova posição na tabela positions."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        position["created_at"] = datetime.utcnow().isoformat()
        position["status"] = "active"

        result = supabase.table("positions").insert(position).execute()

        return {"status": "success", "position": (result.data or [None])[0]}
    except Exception as e:
        logger.error(f"Erro ao criar posição: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# ANÁLISE DE POOL (Fase 1)
# ============================================================

@app.post("/api/analyze/{address}")
async def analyze_pool(address: str, background_tasks: BackgroundTasks):
    """Dispara análise para uma pool específica (em background)."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        result = (
            supabase.table("pools")
            .select("address")
            .eq("address", address.lower())
            .execute()
        )

        if not result.data:
            success = await add_custom_pool(supabase, address.lower())
            if not success:
                raise HTTPException(status_code=404, detail="Pool não encontrada")

        background_tasks.add_task(analyze_single_pool, address.lower())

        return {
            "status": "success",
            "message": f"Análise iniciada para pool {address}",
            "timestamp": datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao iniciar análise: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def analyze_single_pool(address: str):
    """Analisa uma pool específica usando PoolAnalyzer (Fase 1)."""
    if not supabase:
        logger.error("analyze_single_pool chamado sem supabase client")
        return

    try:
        analyzer = PoolAnalyzer(supabase)
        await analyzer.analyze_pool(address)
        logger.info("✅ Análise completa para pool %s", address)
    except Exception as e:
        logger.error(f"❌ Erro ao analisar pool {address}: {e}")


# ============================================================
# DASHBOARD (Fase 1)
# ============================================================

@app.get("/api/dashboard")
async def get_dashboard_data():
    """Retorna dados consolidados para o dashboard principal."""
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase não disponível")

    try:
        pools_result = supabase.table("pools").select("*").execute()
        positions_result = (
            supabase.table("positions")
            .select("*")
            .eq("status", "active")
            .execute()
        )

        pools = pools_result.data or []
        positions = positions_result.data or []

        total_tvl = sum(safe_float(p.get("tvl_usd", 0)) for p in pools)
        total_volume = sum(safe_float(p.get("volume_24h", 0)) for p in pools)

        aprs = [
            safe_float(p.get("fee_apr", 0))
            for p in pools
            if p.get("fee_apr") is not None
        ]
        avg_apr = sum(aprs) / len(aprs) if aprs else 0

        scored_pools = [p for p in pools if p.get("score") is not None]
        top_pools = sorted(
            scored_pools, key=lambda x: safe_int(x.get("score", 0)), reverse=True
        )[:5]

        return {
            "status": "success",
            "metrics": {
                "total_pools": len(pools),
                "active_positions": len(positions),
                "total_tvl": total_tvl,
                "total_volume_24h": total_volume,
                "average_apr": avg_apr,
                "pools_analyzed": len(
                    [p for p in pools if safe_int(p.get("score", 0)) > 0]
                ),
            },
            "top_pools": [
                {
                    "pair": f"{p.get('token0_symbol', 'TOKEN0')}/{p.get('token1_symbol', 'TOKEN1')}",
                    "score": safe_int(p.get("score", 0)),
                    "tvl": safe_float(p.get("tvl_usd", 0)),
                    "apr": safe_float(p.get("fee_apr", 0)),
                }
                for p in top_pools
            ],
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Erro ao buscar dados do dashboard: {e}")
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
                "pools_analyzed": 0,
            },
            "top_pools": [],
            "timestamp": datetime.utcnow().isoformat(),
        }


# ============================================================
# CONFIG / MOTOR DE RISCO (Fase 2)
# ============================================================

@app.get("/api/config")
async def get_user_config():
    """Busca configuração atual do usuário (via EMITYDatabase)."""
    if not db:
        raise HTTPException(status_code=503, detail="Database não disponível")

    try:
        config = db.get_user_config()
        return {"success": True, "config": config}
    except Exception as e:
        logger.error(f"Erro ao buscar config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/config")
async def update_user_config(config_update: ConfigUpdate):
    """Atualiza configuração de risco do usuário."""
    if not db:
        raise HTTPException(status_code=503, detail="Database não disponível")

    try:
        updates = {k: v for k, v in config_update.dict().items() if v is not None}
        if not updates:
            raise HTTPException(status_code=400, detail="Nenhum campo para atualizar")

        if db.update_user_config(updates):
            new_config = db.get_user_config()
            return {
                "success": True,
                "message": "Configuração atualizada",
                "config": new_config,
            }
        else:
            raise HTTPException(status_code=500, detail="Erro ao salvar configuração")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao atualizar config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/position-size")
async def calculate_position_size(request: PositionSizeRequest):
    """Calcula tamanho ideal da posição para uma pool, já validando gas."""
    if not db:
        raise HTTPException(status_code=503, detail="Database não disponível")

    try:
        config = db.get_user_config()
        if not config:
            raise HTTPException(status_code=404, detail="Configuração não encontrada")

        pool = db.get_pool_by_address(request.pool_address)
        if not pool:
            raise HTTPException(status_code=404, detail="Pool não encontrada")

        # anexar simulation_7d a partir de simulations_data da tabela pools
        pool = attach_simulation_7d(pool)

        risk_engine = RiskEngine(config)
        position = risk_engine.calculate_position_size(pool, request.override_pct)

        return {
            "success": True,
            "pool_address": request.pool_address,
            "pair": pool.get("pair") or f"{pool.get('token0_symbol', 'TOKEN0')}/{pool.get('token1_symbol', 'TOKEN1')}",
            "position": position,
            "config": {
                "capital_total": config["capital_total"],
                "perfil_risco": config["perfil_risco"],
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao calcular position size: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/market-check")
async def check_market_conditions():
    """Verifica se as condições de mercado permitem operar, usando RiskEngine."""
    if not db:
        raise HTTPException(status_code=503, detail="Database não disponível")

    try:
        config = db.get_user_config()
        if not config:
            raise HTTPException(status_code=404, detail="Configuração não encontrada")

        pools = db.get_pools(min_score=0, limit=100) or []

        for p in pools:
            attach_simulation_7d(p)

        risk_engine = RiskEngine(config)
        market_check = risk_engine.check_market_conditions(pools)

        return {
            "success": True,
            "timestamp": datetime.utcnow().isoformat(),
            "market": market_check,
        }

    except Exception as e:
        logger.error(f"Erro ao verificar mercado: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/portfolio-allocation")
async def get_portfolio_allocation():
    """Calcula alocação ótima do portfólio com base nas melhores pools."""
    if not db:
        raise HTTPException(status_code=503, detail="Database não disponível")

    try:
        config = db.get_user_config()
        if not config:
            raise HTTPException(status_code=404, detail="Configuração não encontrada")

        pools = db.get_pools(min_score=50, limit=50) or []
        for p in pools:
            attach_simulation_7d(p)

        risk_engine = RiskEngine(config)
        allocation = risk_engine.calculate_portfolio_allocation(pools)

        return {
            "success": True,
            "timestamp": datetime.utcnow().isoformat(),
            "allocation": allocation,
            "config": {
                "capital_total": config["capital_total"],
                "perfil_risco": config["perfil_risco"],
                "max_positions": config["max_positions"],
            },
        }

    except Exception as e:
        logger.error(f"Erro ao calcular alocação: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/sync-position-value")
async def sync_position_value(value: float, value_type: str = "pct"):
    """Sincroniza valores entre % e USDT usando capital_total da config."""
    if not db:
        raise HTTPException(status_code=503, detail="Database não disponível")

    try:
        config = db.get_user_config()
        if not config:
            raise HTTPException(status_code=404, detail="Configuração não encontrada")

        risk_engine = RiskEngine(config)
        pct, usdt = risk_engine.sync_position_values(value, value_type)

        return {
            "success": True,
            "percentage": pct,
            "usdt": usdt,
            "capital_total": config["capital_total"],
        }

    except Exception as e:
        logger.error(f"Erro ao sincronizar valores: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/portfolio-stats")
async def api_get_portfolio_stats():
    """Exposição / PnL agregado do portfólio (usando EMITYDatabase)."""
    if not db:
        raise HTTPException(status_code=503, detail="Database não disponível")

    try:
        stats = db.get_portfolio_stats()
        return {"success": True, "stats": stats}
    except Exception as e:
        logger.error(f"Erro ao buscar estatísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/alerts")
async def get_alerts(limit: int = Query(20, ge=1, le=100)):
    """Busca alertas recentes gerados pelo sistema (tabela alerts)."""
    if not db:
        raise HTTPException(status_code=503, detail="Database não disponível")

    try:
        alerts = db.get_recent_alerts(limit=limit)
        return {
            "success": True,
            "count": len(alerts),
            "alerts": alerts,
        }
    except Exception as e:
        logger.error(f"Erro ao buscar alertas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# STARTUP / SCHEDULED
# ============================================================

@app.on_event("startup")
async def startup_event():
    """Executado ao iniciar a aplicação."""
    logger.info("=" * 60)
    logger.info("🚀 EMITY System iniciando - FASE 3 (Automação + Telegram)")
    logger.info("API rodando...")

    # checar favorite_pools
    if supabase:
        try:
            supabase.table("favorite_pools").select("pool_address").limit(1).execute()
            logger.info("✅ Tabela 'favorite_pools' verificada")
        except Exception:
            logger.warning(
                "⚠️ Tabela 'favorite_pools' não existe. Crie no Supabase com os campos corretos"
            )

    # log da config
    if db:
        try:
            config = db.get_user_config()
            if config:
                logger.info(
                    "📊 Capital Total: $%.2f | Perfil: %s | Max posições: %s",
                    config.get("capital_total", 0),
                    config.get("perfil_risco", "N/A"),
                    config.get("max_positions", "N/A"),
                )
        except Exception:
            pass

    # Log do Telegram
    logger.info(f"🤖 Telegram Bot: {'ATIVADO' if telegram_bot.enabled else 'DESATIVADO'}")

    # agendar scan inicial
    if supabase:
        asyncio.create_task(initial_scan())


async def initial_scan():
    """Scan inicial após startup (espera 60s para evitar cold start)."""
    await asyncio.sleep(60)
    logger.info("🔄 Executando scan inicial...")
    await run_scan_task()


async def scheduled_scanner():
    """Loop opcional de scanner agendado (caso queira rodar em worker separado)."""
    while True:
        try:
            logger.info("⏰ Executando scan agendado...")
            await run_scan_task()
            await asyncio.sleep(1800)  # 30 min
        except Exception as e:
            logger.error(f"Erro no scanner agendado: {e}")
            await asyncio.sleep(300)


# ============================================================
# ENTRYPOINT LOCAL
# ============================================================

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
