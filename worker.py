"""EMITY System - Background Worker
Executa tarefas automáticas: scanner, análises, alertas
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import json
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from database import EMITYDatabase
from scanner import run_scanner
from analyzer import analyze_all_pools
from telegram_bot import telegram_bot, AlertType
from risk_engine import RiskEngine

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class EMITYWorker:
    """Worker principal do sistema EMITY"""
    
    def __init__(self):
        self.db = EMITYDatabase()
        self.supabase = self.db.client
        self.scheduler = AsyncIOScheduler()
        self.last_scan = None
        self.last_alert_check = None
        self.alert_history = {}
        
        # Configurações
        self.scan_interval = int(os.getenv("SCAN_INTERVAL", "300"))  # 5 min
        self.alert_interval = int(os.getenv("ALERT_INTERVAL", "600"))  # 10 min
        self.market_check_interval = int(os.getenv("MARKET_CHECK_INTERVAL", "1800"))  # 30 min
        
    async def run_automated_scan(self):
        """Executa scan automático de pools"""
        try:
            logger.info("🔄 Iniciando scan automático...")
            
            # Executar scanner
            pools = await run_scanner(self.supabase)
            
            if pools:
                logger.info(f"✅ {len(pools)} pools encontradas")
                
                # Analisar todas as pools
                await analyze_all_pools(self.supabase)
                
                # Atualizar timestamp
                self.last_scan = datetime.utcnow()
                
                # Salvar log no banco
                self.supabase.table("alerts").insert({
                    "type": "SYSTEM",
                    "title": "Scan Automático Completo",
                    "message": f"Scan executado com sucesso: {len(pools)} pools analisadas",
                    "severity": "info",
                    "created_at": datetime.utcnow().isoformat()
                }).execute()
                
            else:
                logger.warning("⚠️ Nenhuma pool encontrada no scan")
                
        except Exception as e:
            logger.error(f"❌ Erro no scan automático: {e}")
            
    async def check_opportunities(self):
        """Verifica novas oportunidades (pools com score alto)"""
        try:
            logger.info("🎯 Verificando oportunidades...")
            
            # Buscar pools com score alto
            result = self.supabase.table("pools").select("*").gte("score", 70).execute()
            high_score_pools = result.data or []
            
            for pool in high_score_pools:
                pool_id = pool.get("address")
                
                # Verificar se já alertamos sobre esta pool recentemente
                if pool_id in self.alert_history:
                    last_alert = self.alert_history[pool_id]
                    if datetime.utcnow() - last_alert < timedelta(hours=6):
                        continue
                        
                # Verificar se é uma nova oportunidade
                if pool.get("score", 0) >= 75:
                    # Enviar alerta
                    recommendation = pool.get("recommendation", "Pool com excelente pontuação institucional")
                    await telegram_bot.send_opportunity_alert(pool, recommendation)
                    
                    # Registrar no banco
                    self.supabase.table("alerts").insert({
                        "type": "OPPORTUNITY",
                        "pool_address": pool_id,
                        "title": f"Nova Oportunidade: {pool.get('token0_symbol')}/{pool.get('token1_symbol')}",
                        "message": recommendation,
                        "data": json.dumps({
                            "score": pool.get("score"),
                            "tvl": pool.get("tvl_usd"),
                            "apr": pool.get("fee_apr")
                        }),
                        "severity": "success",
                        "created_at": datetime.utcnow().isoformat()
                    }).execute()
                    
                    # Atualizar histórico
                    self.alert_history[pool_id] = datetime.utcnow()
                    
            logger.info(f"✅ Verificação de oportunidades completa")
            
        except Exception as e:
            logger.error(f"❌ Erro ao verificar oportunidades: {e}")
            
    async def check_risks(self):
        """Verifica riscos em posições ativas"""
        try:
            logger.info("⚠️ Verificando riscos...")
            
            # Buscar posições ativas
            positions = self.supabase.table("positions").select("*").eq("status", "active").execute()
            
            for position in (positions.data or []):
                pool_address = position.get("pool_address")
                
                # Buscar dados atualizados da pool
                pool_result = self.supabase.table("pools").select("*").eq("address", pool_address).execute()
                
                if not pool_result.data:
                    continue
                    
                pool = pool_result.data[0]
                
                # Verificar IL alto
                il_current = pool.get("il_7d", 0)
                if il_current > 5:  # IL > 5%
                    risk_reason = f"Impermanent Loss alto: {il_current:.2f}%"
                    await telegram_bot.send_risk_alert(pool, risk_reason)
                    
                    # Registrar alerta
                    self.supabase.table("alerts").insert({
                        "type": "RISK",
                        "pool_address": pool_address,
                        "title": "Alerta de Risco: IL Alto",
                        "message": risk_reason,
                        "severity": "warning",
                        "created_at": datetime.utcnow().isoformat()
                    }).execute()
                    
                # Verificar gas excessivo
                gas_cost = pool.get("gas_cost", 0)
                expected_return = pool.get("net_return_30d", 0)
                if gas_cost > expected_return * 0.2:  # Gas > 20% do retorno
                    risk_reason = f"Gas cost muito alto: ${gas_cost:.2f} ({(gas_cost/expected_return*100):.1f}% do retorno)"
                    await telegram_bot.send_risk_alert(pool, risk_reason)
                    
            logger.info("✅ Verificação de riscos completa")
            
        except Exception as e:
            logger.error(f"❌ Erro ao verificar riscos: {e}")
            
    async def check_market_conditions(self):
        """Verifica condições gerais do mercado"""
        try:
            logger.info("📊 Verificando condições de mercado...")
            
            # Buscar configuração do usuário
            config = self.db.get_user_config()
            if not config:
                logger.warning("Config não encontrada, usando padrão")
                config = {
                    "capital_total": 10000,
                    "perfil_risco": "moderado",
                    "min_score": 60
                }
                
            # Buscar todas as pools
            pools = self.db.get_pools(min_score=0, limit=100) or []
            
            # Usar RiskEngine para avaliar mercado
            risk_engine = RiskEngine(config)
            market_check = risk_engine.check_market_conditions(pools)
            
            # Enviar alerta sobre condições de mercado
            await telegram_bot.send_market_alert(market_check)
            
            # Registrar no banco
            self.supabase.table("alerts").insert({
                "type": "MARKET",
                "title": f"Mercado: {market_check.get('status', 'Desconhecido')}",
                "message": market_check.get("reason", ""),
                "data": json.dumps(market_check),
                "severity": "info" if market_check.get("should_operate") else "warning",
                "created_at": datetime.utcnow().isoformat()
            }).execute()
            
            logger.info(f"✅ Verificação de mercado completa: {market_check.get('status')}")
            
        except Exception as e:
            logger.error(f"❌ Erro ao verificar mercado: {e}")
            
    async def check_maintenance_needed(self):
        """Verifica posições que precisam de manutenção"""
        try:
            logger.info("🔧 Verificando manutenções necessárias...")
            
            # Buscar posições ativas
            positions = self.supabase.table("positions").select("*").eq("status", "active").execute()
            
            for position in (positions.data or []):
                # Verificar se está fora do range
                time_in_range = position.get("time_in_range", 100)
                
                if time_in_range < 50:  # Menos de 50% do tempo no range
                    action_needed = "Ajustar range - posição está fora do range na maior parte do tempo"
                    await telegram_bot.send_maintenance_alert(position, action_needed)
                    
                    # Registrar
                    self.supabase.table("alerts").insert({
                        "type": "MAINTENANCE",
                        "pool_address": position.get("pool_address"),
                        "title": "Manutenção Necessária",
                        "message": action_needed,
                        "severity": "warning",
                        "created_at": datetime.utcnow().isoformat()
                    }).execute()
                    
            logger.info("✅ Verificação de manutenção completa")
            
        except Exception as e:
            logger.error(f"❌ Erro ao verificar manutenção: {e}")
            
    async def run_all_checks(self):
        """Executa todas as verificações"""
        logger.info("🚀 Executando todas as verificações...")
        
        # Scan
        await self.run_automated_scan()
        await asyncio.sleep(5)
        
        # Verificações
        await self.check_opportunities()
        await asyncio.sleep(2)
        
        await self.check_risks()
        await asyncio.sleep(2)
        
        await self.check_market_conditions()
        await asyncio.sleep(2)
        
        await self.check_maintenance_needed()
        
        logger.info("✅ Todas as verificações completas")
        
    def start(self):
        """Inicia o worker com agendamento"""
        logger.info("=" * 60)
        logger.info("🤖 EMITY Worker iniciando...")
        logger.info(f"📅 Scan a cada {self.scan_interval}s")
        logger.info(f"🔔 Alertas a cada {self.alert_interval}s")
        logger.info(f"📊 Market check a cada {self.market_check_interval}s")
        logger.info("=" * 60)
        
        # Agendar tarefas
        self.scheduler.add_job(
            self.run_automated_scan,
            'interval',
            seconds=self.scan_interval,
            id='automated_scan',
            max_instances=1
        )
        
        self.scheduler.add_job(
            self.check_opportunities,
            'interval',
            seconds=self.alert_interval,
            id='check_opportunities',
            max_instances=1
        )
        
        self.scheduler.add_job(
            self.check_risks,
            'interval',
            seconds=self.alert_interval,
            id='check_risks',
            max_instances=1
        )
        
        self.scheduler.add_job(
            self.check_market_conditions,
            'interval',
            seconds=self.market_check_interval,
            id='market_check',
            max_instances=1
        )
        
        # Iniciar scheduler
        self.scheduler.start()
        
        # Executar primeira verificação em 30 segundos
        asyncio.create_task(self.initial_check())
        
    async def initial_check(self):
        """Primeira verificação após iniciar"""
        logger.info("⏳ Aguardando 30s para primeira verificação...")
        await asyncio.sleep(30)
        await self.run_all_checks()
        
    async def keep_alive(self):
        """Mantém o worker vivo"""
        try:
            while True:
                await asyncio.sleep(60)
                logger.debug("💗 Worker alive...")
        except KeyboardInterrupt:
            logger.info("🛑 Worker interrompido")
            self.scheduler.shutdown()


# Função para rodar o worker
async def run_worker():
    """Função principal para executar o worker"""
    worker = EMITYWorker()
    worker.start()
    await worker.keep_alive()


if __name__ == "__main__":
    # Executar worker
    asyncio.run(run_worker())
