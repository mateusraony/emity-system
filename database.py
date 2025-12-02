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
        CREATE TABLE IF NOT EXISTS pools (
            id SERIAL PRIMARY KEY,
            address VARCHAR(255) UNIQUE NOT NULL,
            token0_symbol VARCHAR(50),
            token0_address VARCHAR(255),
            token1_symbol VARCHAR(50),
            token1_address VARCHAR(255),
            fee_tier DECIMAL(10, 4),
            tvl_usd DECIMAL(20, 2),
            volume_24h DECIMAL(20, 2),
            fees_24h DECIMAL(20, 2),
            current_tick INTEGER,
            current_price DECIMAL(20, 8),
            price_change_24h DECIMAL(10, 2),
            fee_apr DECIMAL(10, 2),
            volatility DECIMAL(10, 2),
            il_7d DECIMAL(10, 2),
            il_30d DECIMAL(10, 2),
            score INTEGER DEFAULT 0,
            recommendation TEXT,
            explanation TEXT,
            ranges_data JSONB,
            simulations_data JSONB,
            last_updated TIMESTAMP DEFAULT NOW(),
            last_analyzed TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW()
        );

        -- Tabela de Posições do Usuário
        CREATE TABLE IF NOT EXISTS positions (
            id SERIAL PRIMARY KEY,
            pool_address VARCHAR(255),
            pair_name VARCHAR(100),
            capital_usd DECIMAL(20, 2),
            range_min DECIMAL(20, 8),
            range_max DECIMAL(20, 8),
            entry_date TIMESTAMP DEFAULT NOW(),
            exit_date TIMESTAMP,
            status VARCHAR(50) DEFAULT 'active',
            pnl_usd DECIMAL(20, 2),
            fees_earned DECIMAL(20, 2),
            il_realized DECIMAL(20, 2),
            gas_spent DECIMAL(10, 2),
            created_at TIMESTAMP DEFAULT NOW()
        );

        -- Tabela de Alertas
        CREATE TABLE IF NOT EXISTS alerts (
            id SERIAL PRIMARY KEY,
            alert_type VARCHAR(50),
            pool_address VARCHAR(255),
            message TEXT,
            severity VARCHAR(20),
            sent_telegram BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        );

        -- Tabela de Configurações
        CREATE TABLE IF NOT EXISTS config (
            id SERIAL PRIMARY KEY,
            config_key VARCHAR(100) UNIQUE NOT NULL,
            config_value TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- Índices para performance
        CREATE INDEX IF NOT EXISTS idx_pools_score ON pools(score DESC);
        CREATE INDEX IF NOT EXISTS idx_pools_tvl ON pools(tvl_usd DESC);
        CREATE INDEX IF NOT EXISTS idx_pools_volume ON pools(volume_24h DESC);
        CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
        CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_at DESC);
        """
        
        return sql_setup
    
    async def get_all_pools(self) -> List[Dict]:
        """Retorna todas as pools ativas"""
        try:
            response = self.supabase.table('pools').select("*").execute()
            return response.data
        except Exception as e:
            print(f"Erro ao buscar pools: {e}")
            return []
    
    async def upsert_pool(self, pool_data: Dict) -> bool:
        """Insere ou atualiza uma pool"""
        try:
            pool_data['last_updated'] = datetime.now().isoformat()
            response = self.supabase.table('pools').upsert(pool_data).execute()
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
            response = self.supabase.table('alerts').insert(alert_data).execute()
            return True
        except Exception as e:
            print(f"Erro ao salvar alerta: {e}")
            return False
    
    async def get_config(self, key: str) -> Optional[str]:
        """Busca uma configuração"""
        try:
            response = self.supabase.table('config').select("config_value").eq('config_key', key).execute()
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
            response = self.supabase.table('config').upsert(config_data).execute()
            return True
        except Exception as e:
            print(f"Erro ao salvar config: {e}")
            return False

# Instância global
db = DatabaseManager()

def get_supabase_client() -> Client:
    """
    Função para obter o cliente Supabase
    Usada pelo scanner e analyzer
    """
    return create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_KEY")
    )
