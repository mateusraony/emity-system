"""
EMITY Database Manager - Supabase Integration
Gestão completa das tabelas do sistema
"""

import os
from typing import List, Dict, Optional
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

class DatabaseManager:
    def __init__(self):
        """Inicializa conexão com Supabase"""
        self.supabase: Client = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_KEY")
        )
        
    def setup_tables(self):
        """
        Cria estrutura das tabelas no Supabase
        Execute estas queries no SQL Editor do Supabase
        """
        sql_setup = """
        -- Tabela de Pools Monitoradas
        CREATE TABLE IF NOT EXISTS emity_pools (
            id SERIAL PRIMARY KEY,
            pool_address VARCHAR(255) UNIQUE NOT NULL,
            dex VARCHAR(100) NOT NULL,
            chain VARCHAR(100) NOT NULL,
            token0_symbol VARCHAR(50),
            token0_address VARCHAR(255),
            token1_symbol VARCHAR(50),
            token1_address VARCHAR(255),
            tvl_usd DECIMAL(20, 2),
            volume_24h_usd DECIMAL(20, 2),
            fee_tier DECIMAL(10, 4),
            apr_7d DECIMAL(10, 2),
            apr_30d DECIMAL(10, 2),
            il_7d DECIMAL(10, 2),
            il_30d DECIMAL(10, 2),
            score_institutional DECIMAL(5, 2),
            score_explanation TEXT,
            status VARCHAR(50) DEFAULT 'active',
            last_update TIMESTAMP DEFAULT NOW(),
            created_at TIMESTAMP DEFAULT NOW()
        );

        -- Tabela de Ranges Recomendados
        CREATE TABLE IF NOT EXISTS emity_ranges (
            id SERIAL PRIMARY KEY,
            pool_address VARCHAR(255) REFERENCES emity_pools(pool_address),
            range_type VARCHAR(50), -- 'defensive', 'optimized', 'aggressive'
            lower_price DECIMAL(20, 8),
            upper_price DECIMAL(20, 8),
            expected_apr DECIMAL(10, 2),
            expected_il DECIMAL(10, 2),
            time_in_range_pct DECIMAL(5, 2),
            capital_efficiency DECIMAL(10, 2),
            created_at TIMESTAMP DEFAULT NOW()
        );

        -- Tabela de Posições do Usuário
        CREATE TABLE IF NOT EXISTS emity_positions (
            id SERIAL PRIMARY KEY,
            pool_address VARCHAR(255) REFERENCES emity_pools(pool_address),
            position_type VARCHAR(50), -- 'active', 'pending', 'closed'
            capital_usd DECIMAL(20, 2),
            range_lower DECIMAL(20, 8),
            range_upper DECIMAL(20, 8),
            entry_date TIMESTAMP,
            exit_date TIMESTAMP,
            pnl_usd DECIMAL(20, 2),
            fees_earned_usd DECIMAL(20, 2),
            il_realized_usd DECIMAL(20, 2),
            gas_spent_usd DECIMAL(10, 2),
            created_at TIMESTAMP DEFAULT NOW()
        );

        -- Tabela de Alertas
        CREATE TABLE IF NOT EXISTS emity_alerts (
            id SERIAL PRIMARY KEY,
            alert_type VARCHAR(50), -- 'opportunity', 'risk', 'maintenance', 'position'
            pool_address VARCHAR(255),
            message TEXT,
            severity VARCHAR(20), -- 'low', 'medium', 'high', 'critical'
            sent_telegram BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        );

        -- Tabela de Configurações do Usuário
        CREATE TABLE IF NOT EXISTS emity_config (
            id SERIAL PRIMARY KEY,
            config_key VARCHAR(100) UNIQUE NOT NULL,
            config_value TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- Índices para performance
        CREATE INDEX idx_pools_score ON emity_pools(score_institutional DESC);
        CREATE INDEX idx_pools_tvl ON emity_pools(tvl_usd DESC);
        CREATE INDEX idx_pools_volume ON emity_pools(volume_24h_usd DESC);
        CREATE INDEX idx_positions_status ON emity_positions(position_type);
        CREATE INDEX idx_alerts_created ON emity_alerts(created_at DESC);
        """
        
        return sql_setup
    
    async def get_all_pools(self) -> List[Dict]:
        """Retorna todas as pools ativas"""
        try:
            response = self.supabase.table('emity_pools').select("*").eq('status', 'active').execute()
            return response.data
        except Exception as e:
            print(f"Erro ao buscar pools: {e}")
            return []
    
    async def upsert_pool(self, pool_data: Dict) -> bool:
        """Insere ou atualiza uma pool"""
        try:
            pool_data['last_update'] = datetime.now().isoformat()
            response = self.supabase.table('emity_pools').upsert(pool_data).execute()
            return True
        except Exception as e:
            print(f"Erro ao upsert pool: {e}")
            return False
    
    async def save_alert(self, alert_type: str, message: str, pool_address: str = None, severity: str = 'medium') -> bool:
        """Salva um alerta no banco"""
        try:
            alert_data = {
                'alert_type': alert_type,
                'message': message,
                'pool_address': pool_address,
                'severity': severity,
                'created_at': datetime.now().isoformat()
            }
            response = self.supabase.table('emity_alerts').insert(alert_data).execute()
            return True
        except Exception as e:
            print(f"Erro ao salvar alerta: {e}")
            return False
    
    async def get_config(self, key: str) -> Optional[str]:
        """Busca uma configuração"""
        try:
            response = self.supabase.table('emity_config').select("config_value").eq('config_key', key).execute()
            if response.data:
                return response.data[0]['config_value']
            return None
        except Exception as e:
            print(f"Erro ao buscar config: {e}")
            return None
    
    async def set_config(self, key: str, value: str) -> bool:
        """Define uma configuração"""
        try:
            config_data = {
                'config_key': key,
                'config_value': value,
                'updated_at': datetime.now().isoformat()
            }
            response = self.supabase.table('emity_config').upsert(config_data).execute()
            return True
        except Exception as e:
            print(f"Erro ao salvar config: {e}")
            return False

# Instância global
db = DatabaseManager()
