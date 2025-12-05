"""
EMITY System - Conexão e operações com Supabase
Versão com suporte a user_config (Fase 2)
"""
import os
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class EMITYDatabase:
    def __init__(self):
        """Inicializa conexão com Supabase"""

        # URL é sempre SUPABASE_URL
        supabase_url = os.getenv("SUPABASE_URL")

        # Compatível com o que você já tinha:
        # tenta SUPABASE_ANON_KEY, depois SUPABASE_KEY, depois SERVICE_ROLE
        supabase_key = (
            os.getenv("SUPABASE_ANON_KEY")
            or os.getenv("SUPABASE_KEY")
            or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        )

        if not supabase_url or not supabase_key:
            logger.error(
                "SUPABASE_URL/SUPABASE_KEY não encontrados. "
                "Configure SUPABASE_URL e SUPABASE_ANON_KEY (ou SUPABASE_KEY) no Render."
            )
            raise ValueError(
                "SUPABASE_URL e chave do Supabase devem estar configurados"
            )

        self.client: Client = create_client(supabase_url, supabase_key)
        logger.info("Conectado ao Supabase")

    # ============= POOLS (Fase 1) =============
    def upsert_pool(self, pool_data: Dict) -> bool:
        """Insere ou atualiza uma pool"""
        try:
            # Na sua tabela o campo é 'address'
            if not pool_data.get("address"):
                logger.error("address é obrigatório em pools")
                return False

            # Converter None para valores padrão
            pool_data = {k: (v if v is not None else 0) for k, v in pool_data.items()}

            self.client.table("pools").upsert(pool_data).execute()
            return True
        except Exception as e:
            logger.error(f"Erro ao upsert pool: {e}")
            return False

    def get_pools(self, min_score: int = 0, limit: int = 100) -> List[Dict]:
        """Busca pools com filtros"""
        try:
            query = self.client.table("pools").select("*")

            if min_score > 0:
                query = query.gte("score", min_score)

            result = query.order("score", desc=True).limit(limit).execute()
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"Erro ao buscar pools: {e}")
            return []

    def get_pool_by_address(self, pool_address: str) -> Optional[Dict]:
        """Busca uma pool específica"""
        try:
            # No schema 'pools' o campo é 'address'
            result = (
                self.client.table("pools")
                .select("*")
                .eq("address", pool_address)
                .execute()
            )
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Erro ao buscar pool {pool_address}: {e}")
            return None

    # ============= ANÁLISES (Fase 1) =============
    def save_analysis(self, analysis_data: Dict) -> bool:
        """Salva análise de uma pool"""
        try:
            # Converter None para valores padrão
            analysis_data = {
                k: (v if v is not None else 0) for k, v in analysis_data.items()
            }

            self.client.table("analyses").insert(analysis_data).execute()
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar análise: {e}")
            return False

    def get_latest_analysis(self, pool_address: str) -> Optional[Dict]:
        """Busca última análise de uma pool"""
        try:
            result = (
                self.client.table("analyses")
                .select("*")
                .eq("pool_address", pool_address)
                .order("analyzed_at", desc=True)
                .limit(1)
                .execute()
            )
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Erro ao buscar análise: {e}")
            return None

    # ============= USER CONFIG (Fase 2 - NOVO) =============
    def get_user_config(self) -> Optional[Dict]:
        """Busca configuração atual do usuário"""
        try:
            result = (
                self.client.table("user_config")
                .select("*")
                .order("updated_at", desc=True)
                .limit(1)
                .execute()
            )
            if result.data:
                config = result.data[0]
                # Converter strings para números onde necessário
                config["capital_total"] = float(config.get("capital_total", 10000))
                config["max_positions"] = int(config.get("max_positions", 3))
                config["stop_loss"] = float(config.get("stop_loss", 10.0))
                config["max_position_size"] = float(
                    config.get("max_position_size", 30.0)
                )
                config["min_score"] = int(config.get("min_score", 60))
                config["gas_multiplier"] = float(config.get("gas_multiplier", 2.0))
                return config

            # Se não existe, criar configuração padrão
            return self.create_default_config()

        except Exception as e:
            logger.error(f"Erro ao buscar configuração: {e}")
            return self.create_default_config()

    def create_default_config(self) -> Dict:
        """Cria configuração padrão"""
        default_config = {
            "capital_total": 10000,
            "perfil_risco": "conservador",
            "max_positions": 3,
            "stop_loss": 10.0,
            "max_position_size": 30.0,
            "min_score": 60,
            "gas_multiplier": 2.0,
        }

        try:
            result = (
                self.client.table("user_config").insert(default_config).execute()
            )
            if result.data:
                return result.data[0]
        except Exception as e:
            logger.error(f"Erro ao criar config padrão: {e}")

        return default_config

    def update_user_config(self, config_updates: Dict) -> bool:
        """Atualiza configuração do usuário"""
        try:
            # Buscar config atual para pegar o ID
            current = self.get_user_config()
            if not current or not current.get("id"):
                # Se não existe, criar nova
                self.client.table("user_config").insert(config_updates).execute()
            else:
                # Atualizar existente
                config_updates["updated_at"] = datetime.utcnow().isoformat()
                (
                    self.client.table("user_config")
                    .update(config_updates)
                    .eq("id", current["id"])
                    .execute()
                )

            # Salvar no histórico se houver mudanças significativas
            if current:
                self._save_config_history(current, config_updates)

            return True

        except Exception as e:
            logger.error(f"Erro ao atualizar configuração: {e}")
            return False

    def _save_config_history(self, old_config: Dict, new_values: Dict):
        """Salva histórico de mudanças na configuração"""
        try:
            for field, new_value in new_values.items():
                if field in ["updated_at", "created_at", "id"]:
                    continue

                old_value = old_config.get(field)
                if str(old_value) != str(new_value):
                    history_entry = {
                        "config_id": old_config.get("id"),
                        "field_changed": field,
                        "old_value": str(old_value),
                        "new_value": str(new_value),
                    }
                    self.client.table("config_history").insert(history_entry).execute()

        except Exception as e:
            logger.error(f"Erro ao salvar histórico: {e}")

    def get_config_history(self, limit: int = 50) -> List[Dict]:
        """Busca histórico de mudanças na configuração"""
        try:
            result = (
                self.client.table("config_history")
                .select("*")
                .order("changed_at", desc=True)
                .limit(limit)
                .execute()
            )
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"Erro ao buscar histórico: {e}")
            return []

    # ============= POSIÇÕES ATIVAS (Fase 2 - NOVO) =============
    def get_active_positions(self) -> List[Dict]:
        """Busca posições ativas do usuário (simulado por enquanto)"""
        try:
            pools = self.get_pools(min_score=70, limit=3)

            positions = []
            capital_per_position = 10000  # Exemplo

            for pool in pools[:3]:  # Máximo 3 posições
                positions.append(
                    {
                        "pool_address": pool.get("address"),
                        "pair": pool.get("pair", "N/A"),
                        "capital_invested": capital_per_position,
                        "current_value": capital_per_position * 1.05,  # Simular 5% de lucro
                        "pnl": capital_per_position * 0.05,
                        "pnl_percentage": 5.0,
                        "status": "active",
                        "opened_at": datetime.utcnow() - timedelta(days=2),
                    }
                )

            return positions

        except Exception as e:
            logger.error(f"Erro ao buscar posições ativas: {e}")
            return []

    # ============= ALERTAS (Fase 2 - NOVO) =============
    def save_alert(self, alert_data: Dict) -> bool:
        """Salva um alerta gerado pelo sistema"""
        try:
            record = {
                "alert_type": alert_data.get("type")
                or alert_data.get("alert_type")
                or "info",
                "pool_address": alert_data.get("pool_address"),
                # se não houver coluna title, incorporamos no message
                "message": alert_data.get("message")
                or alert_data.get("title")
                or "",
                "severity": alert_data.get("severity", "low"),
                "created_at": datetime.utcnow().isoformat(),
            }
            self.client.table("alerts").insert(record).execute()
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar alerta: {e}")
            return False

    def get_recent_alerts(self, limit: int = 20) -> List[Dict]:
        """Busca alertas recentes"""
        try:
            result = (
                self.client.table("alerts")
                .select("*")
                .order("created_at", desc=True)
                .limit(limit)
                .execute()
            )
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"Erro ao buscar alertas: {e}")
            return []

    # ============= ESTATÍSTICAS (Fase 2 - NOVO) =============
    def get_portfolio_stats(self) -> Dict:
        """Calcula estatísticas do portfolio"""
        try:
            positions = self.get_active_positions()

            total_invested = sum(p.get("capital_invested", 0) for p in positions)
            total_value = sum(p.get("current_value", 0) for p in positions)
            total_pnl = total_value - total_invested
            total_pnl_pct = (
                (total_pnl / total_invested * 100) if total_invested > 0 else 0
            )

            return {
                "total_positions": len(positions),
                "total_invested": round(total_invested, 2),
                "total_value": round(total_value, 2),
                "total_pnl": round(total_pnl, 2),
                "total_pnl_percentage": round(total_pnl_pct, 2),
                "best_performer": max(
                    positions, key=lambda x: x.get("pnl_percentage", 0)
                )
                if positions
                else None,
                "worst_performer": min(
                    positions, key=lambda x: x.get("pnl_percentage", 0)
                )
                if positions
                else None,
            }

        except Exception as e:
            logger.error(f"Erro ao calcular estatísticas: {e}")
            return {
                "total_positions": 0,
                "total_invested": 0,
                "total_value": 0,
                "total_pnl": 0,
                "total_pnl_percentage": 0,
            }
