"""EMITY System - Módulo Telegram Bot
Gerencia envio de alertas e notificações via Telegram
"""

import os
import json
import logging
import asyncio
from datetime import datetime
from typing import Optional, Dict, List
import httpx
from enum import Enum

logger = logging.getLogger(__name__)

class AlertType(Enum):
    """Tipos de alertas do sistema"""
    OPPORTUNITY = "🎯 OPORTUNIDADE"
    RISK = "⚠️ RISCO"
    MAINTENANCE = "🔧 MANUTENÇÃO"
    MARKET = "📊 MERCADO"
    SYSTEM = "💻 SISTEMA"
    POSITION = "📍 POSIÇÃO"
    
class TelegramBot:
    """Bot do Telegram para EMITY System"""
    
    def __init__(self, token: str = None, chat_id: str = None):
        self.token = token or os.getenv("TELEGRAM_BOT_TOKEN", "7530029075:AAHnQtsx0G08J9ARzouaAdH4skimhCBdCUo")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "1411468886")
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.enabled = os.getenv("TELEGRAM_ENABLED", "true").lower() == "true"
        self.dashboard_url = os.getenv("DASHBOARD_URL", "https://emity-system.onrender.com")
        
    async def send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """Envia mensagem para o Telegram"""
        if not self.enabled:
            logger.info("Telegram desabilitado, mensagem não enviada")
            return False
            
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/sendMessage",
                    json={
                        "chat_id": self.chat_id,
                        "text": text,
                        "parse_mode": parse_mode,
                        "disable_web_page_preview": False
                    },
                    timeout=10
                )
                
                if response.status_code == 200:
                    logger.info("✅ Mensagem enviada para Telegram")
                    return True
                else:
                    logger.error(f"❌ Erro ao enviar mensagem: {response.text}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Erro ao enviar mensagem Telegram: {e}")
            return False
            
    async def send_alert(self, alert_type: AlertType, title: str, content: Dict, pool_data: Optional[Dict] = None) -> bool:
        """Envia alerta formatado para o Telegram"""
        try:
            # Construir mensagem
            message = f"<b>{alert_type.value}</b>\n"
            message += f"<b>{title}</b>\n"
            message += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            
            # Adicionar dados da pool se disponível
            if pool_data:
                pair = f"{pool_data.get('token0_symbol', 'TOKEN0')}/{pool_data.get('token1_symbol', 'TOKEN1')}"
                message += f"🪙 <b>Pool:</b> {pair}\n"
                message += f"💰 <b>TVL:</b> ${pool_data.get('tvl_usd', 0):,.0f}\n"
                message += f"📊 <b>Volume 24h:</b> ${pool_data.get('volume_24h', 0):,.0f}\n"
                message += f"⭐ <b>Score:</b> {pool_data.get('score', 0)}/100\n\n"
                
            # Adicionar conteúdo específico do alerta
            for key, value in content.items():
                if value is not None:
                    # Formatar chave
                    formatted_key = key.replace("_", " ").title()
                    
                    # Formatar valor
                    if isinstance(value, (int, float)):
                        if "pct" in key or "percent" in key or "apr" in key:
                            formatted_value = f"{value:.2f}%"
                        elif "usd" in key or "capital" in key or "value" in key:
                            formatted_value = f"${value:,.2f}"
                        else:
                            formatted_value = f"{value:,.2f}"
                    else:
                        formatted_value = str(value)
                        
                    message += f"• <b>{formatted_key}:</b> {formatted_value}\n"
                    
            # Adicionar link para o dashboard
            message += f"\n🔗 <a href='{self.dashboard_url}'>Acessar Dashboard</a>\n"
            message += f"\n⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
            
            # Enviar mensagem
            return await self.send_message(message)
            
        except Exception as e:
            logger.error(f"Erro ao enviar alerta: {e}")
            return False
            
    async def send_opportunity_alert(self, pool: Dict, recommendation: str) -> bool:
        """Envia alerta de oportunidade (nova pool com score alto)"""
        content = {
            "recomendação": recommendation,
            "fee_apr": pool.get("fee_apr", 0),
            "il_7d": pool.get("il_7d", 0),
            "net_return_30d": pool.get("net_return_30d", 0),
            "capital_sugerido": pool.get("suggested_capital", 1000)
        }
        
        return await self.send_alert(
            AlertType.OPPORTUNITY,
            "Nova Pool Recomendada!",
            content,
            pool
        )
        
    async def send_risk_alert(self, pool: Dict, risk_reason: str) -> bool:
        """Envia alerta de risco (IL alto, gas excessivo, etc)"""
        content = {
            "motivo": risk_reason,
            "il_atual": pool.get("il_current", 0),
            "gas_cost": pool.get("gas_cost", 0),
            "perda_estimada": pool.get("estimated_loss", 0),
            "ação_sugerida": "Considere fechar posição ou ajustar range"
        }
        
        return await self.send_alert(
            AlertType.RISK,
            "Alerta de Risco Detectado!",
            content,
            pool
        )
        
    async def send_maintenance_alert(self, position: Dict, action_needed: str) -> bool:
        """Envia alerta de manutenção de posição"""
        content = {
            "posição": position.get("pool_address", ""),
            "range_atual": f"{position.get('min_price', 0):.4f} - {position.get('max_price', 0):.4f}",
            "tempo_em_range": f"{position.get('time_in_range', 0):.1f}%",
            "ação_necessária": action_needed,
            "urgência": position.get("urgency", "Média")
        }
        
        return await self.send_alert(
            AlertType.MAINTENANCE,
            "Manutenção de Posição Necessária",
            content
        )
        
    async def send_market_alert(self, market_status: Dict) -> bool:
        """Envia alerta sobre condições de mercado"""
        if market_status.get("should_operate"):
            title = "✅ Mercado Favorável para Operar"
            emoji = "🟢"
        else:
            title = "❌ Mercado Desfavorável - NÃO OPERAR"
            emoji = "🔴"
            
        content = {
            "status": f"{emoji} {market_status.get('status', 'Desconhecido')}",
            "pools_viáveis": market_status.get("viable_pools", 0),
            "score_médio": market_status.get("avg_score", 0),
            "motivo": market_status.get("reason", ""),
            "próxima_verificação": "30 minutos"
        }
        
        return await self.send_alert(
            AlertType.MARKET,
            title,
            content
        )
        
    async def send_test_message(self) -> bool:
        """Envia mensagem de teste para verificar configuração"""
        message = """
<b>🧪 TESTE - EMITY System</b>
━━━━━━━━━━━━━━━━━━━━━━

✅ <b>Bot configurado com sucesso!</b>

Sistema de alertas ativado para:
- 🎯 Oportunidades (pools com score > 70)
- ⚠️ Riscos (IL alto, gas excessivo)
- 🔧 Manutenção (ajuste de ranges)
- 📊 Condições de mercado
- 💻 Status do sistema

Os alertas serão enviados automaticamente
a cada 5-10 minutos quando houver novidades.

🔗 <a href='https://emity-system.onrender.com'>Acessar Dashboard</a>

⏰ {timestamp}
""".format(timestamp=datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'))
        
        return await self.send_message(message)
        
    async def send_batch_alerts(self, alerts: List[Dict]) -> int:
        """Envia múltiplos alertas em batch"""
        sent_count = 0
        
        for alert in alerts:
            alert_type = AlertType[alert.get("type", "SYSTEM")]
            success = await self.send_alert(
                alert_type,
                alert.get("title", "Alerta EMITY"),
                alert.get("content", {}),
                alert.get("pool_data")
            )
            
            if success:
                sent_count += 1
                
            # Delay entre mensagens para evitar rate limit
            await asyncio.sleep(1)
            
        logger.info(f"📤 Enviados {sent_count}/{len(alerts)} alertas")
        return sent_count
        
    def format_number(self, value: float, decimals: int = 2, is_currency: bool = False) -> str:
        """Formata números para exibição"""
        if is_currency:
            return f"${value:,.{decimals}f}"
        return f"{value:,.{decimals}f}"
        
    def format_percentage(self, value: float, decimals: int = 2) -> str:
        """Formata percentuais para exibição"""
        return f"{value:.{decimals}f}%"


# Singleton para uso global
telegram_bot = TelegramBot()
