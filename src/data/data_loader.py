import pandas as pd
import numpy as np
from pathlib import Path
import glob
import os

class CreditCardDataLoader:
    def __init__(self, data_path):
        self.data_path = Path(data_path).resolve()  # 절대 경로로 변환
        self.data = {}
    
    def _find_parquet_files(self, directory):
        """디렉토리에서 모든 parquet 파일 찾기"""
        directory = str(directory)  # Path 객체를 문자열로 변환
        pattern = os.path.join(directory, "*.parquet")
        files = glob.glob(pattern)
        if not files:
            raise FileNotFoundError(f"No parquet file found in {directory}")
        return files
    
    def _load_parquet_files(self, directory):
        """디렉토리의 모든 parquet 파일을 하나의 DataFrame으로 로드"""
        files = self._find_parquet_files(directory)
        dfs = []
        for file in files:
            print(f"Loading {file}")  # 로딩 중인 파일 출력
            df = pd.read_parquet(file)
            dfs.append(df)
        return pd.concat(dfs, ignore_index=True)
    
    def load_member_data(self):
        """회원 정보 데이터 로드"""
        self.data['member'] = self._load_parquet_files(self.data_path / '1.회원정보')
        return self.data['member']
    
    def load_credit_data(self):
        """신용 정보 데이터 로드"""
        self.data['credit'] = self._load_parquet_files(self.data_path / '2.신용정보')
        return self.data['credit']
    
    def load_transaction_data(self):
        """승인매출 정보 데이터 로드"""
        self.data['transaction'] = self._load_parquet_files(self.data_path / '3.승인매출정보')
        return self.data['transaction']
    
    def load_payment_data(self):
        """청구입금 정보 데이터 로드"""
        self.data['payment'] = self._load_parquet_files(self.data_path / '4.청구입금정보')
        return self.data['payment']
    
    def load_balance_data(self):
        """잔액 정보 데이터 로드"""
        self.data['balance'] = self._load_parquet_files(self.data_path / '5.잔액정보')
        return self.data['balance']
    
    def load_channel_data(self):
        """채널 정보 데이터 로드"""
        self.data['channel'] = self._load_parquet_files(self.data_path / '6.채널정보')
        return self.data['channel']
    
    def load_marketing_data(self):
        """마케팅 정보 데이터 로드"""
        self.data['marketing'] = self._load_parquet_files(self.data_path / '7.마케팅정보')
        return self.data['marketing']
    
    def load_performance_data(self):
        """성과 정보 데이터 로드"""
        self.data['performance'] = self._load_parquet_files(self.data_path / '8.성과정보')
        return self.data['performance']
    
    def load_all_data(self):
        """모든 데이터 로드"""
        try:
            print(f"Loading data from: {self.data_path}")  # 데이터 경로 출력
            self.load_member_data()
            self.load_credit_data()
            self.load_transaction_data()
            self.load_payment_data()
            self.load_balance_data()
            self.load_channel_data()
            self.load_marketing_data()
            self.load_performance_data()
            print("모든 데이터가 성공적으로 로드되었습니다.")
        except Exception as e:
            print(f"데이터 로드 중 오류 발생: {str(e)}")
        return self.data
    
    def get_data_info(self):
        """각 데이터셋의 기본 정보 출력"""
        info = {}
        for name, df in self.data.items():
            info[name] = {
                'shape': df.shape,
                'columns': df.columns.tolist(),
                'dtypes': df.dtypes.to_dict(),
                'missing_values': df.isnull().sum().to_dict()
            }
        return info
