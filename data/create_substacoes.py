import pandas as pd

# Dados fornecidos
data = {
    'Sigla': ['JAN', 'SAB', 'ABV', 'AGU', 'ALP', 'ALV', 'AME', 'AMR', 'ANA', 'ANB', 'ANC', 'ASE', 'AUT', 'BAI', 'BAL', 'BAR', 'BAT', 'BAV', 
              'BFU', 'BJU', 'BRA', 'BRU', 'BSI', 'BUT', 'CAA', 'CAC', 'CAI', 'CAL', 'CAM', 'CAP', 'CAT', 'CLA', 'CLE', 'COG', 'COI', 'CON', 
              'COT', 'CPE', 'CPI', 'CRA', 'CTA', 'CTL', 'CUP', 'CVE', 'DIA', 'EGU', 'EMB', 'ERM', 'ESP', 'GCA', 'GER', 'GJU', 'GNA', 'GPR', 
              'GUA', 'GUM', 'GVI', 'HIP', 'IMG', 'ITA', 'ITN', 'ITP', 'ITR', 'IVI', 'JAC', 'JAG', 'JCI', 'JGL', 'JKU', 'JOR', 'JUQ', 'LAP', 
              'LEO', 'LIM', 'LUB', 'MAD', 'MAT', 'MAU', 'MAZ', 'MBE', 'MEN', 'MNO', 'MOC', 'MON', 'MOO', 'MOR', 'MPA', 'MSA', 'NAC', 'NMU', 
              'ORA', 'OSA', 'PAN', 'PAR', 'PEN', 'PER', 'PIP', 'PLA', 'PNH', 'PPO', 'PPR', 'PPU', 'PRE', 'PRI', 'PSD', 'RBO', 'REM', 'RGR', 
              'ROS', 'RPI', 'RTA', 'SAC', 'SAM', 'SAU', 'SBC', 'SER', 'SIL', 'SJO', 'SLO', 'SMA', 'SND', 'SUM', 'TAI', 'TAM', 'TIR', 'TMO', 
              'TMR', 'TSE', 'TTI', 'TUC', 'UTI', 'VAL', 'VAR', 'VEM', 'VFO', 'VGR', 'VGU', 'VIT', 'VMA', 'VME', 'VPA', 'VPC', 'VPR', 'VTA'],
    'Nome': ['JANDIRA', 'SABARÁ', 'ALTO BOA VISTA', 'ALEXANDRE DE GUSMÃO', 'ALPHAVILLE', 'ALVARENGA', 'AMERICANÓPOLIS', 'AMÉRICA', 'ANASTÁCIO',
             'ANHEMBI', 'ANCHIETA', 'ALDEIA DA SERRA', 'AUTONOMISTAS', 'BUENOS AIRES', 'BELA ALIANÇA', 'BARTIRA', 'BATISTINI', 'BAVIERA', 
             'BARRA FUNDA', 'BOM JESUS', 'BRÁS', 'BARUERI', 'BRASILÂNDIA', 'BUTANTÃ', 'CANAÃ', 'CAUCAIA', 'CANINDÉ', 'CAPELA', 'CAMBUCI', 
             'CAPUAVA', 'CATUMBI', 'CLÁUDIA', 'CLEMENTINO', 'CONGONHAS', 'COIMBRA', 'CONTINENTAL', 'COTIA', 'CAMPESTRE', 'CARAPICUIBA', 'CARRÃO', 
             'COMANDANTE TAYLOR', 'CASTELO', 'CUPECÊ', 'CASA VERDE', 'DIADEMA', 'EMBU-GUAÇU(CTEEP)', 'EMBÚ', 'ERMELINO MATARAZO', 'ESPLANADA', 
             'GOMES CARDIM', 'GERMÂNIA', 'GRANJA JULIETA', 'GUAIANAZES', 'GATO PRETO', 'GUARAPIRANGA', 'GUMERCINDO', 'GRANJA VIANA', 'HIPÓDROMO', 
             'IMIGRANTES', 'ITAIM', 'ITAQUERUNA', 'ITAPECERICA', 'ITAQUERA', 'ITAPEVI', 'JAÇANÃ', 'JAGUARÉ', 'JOÃO CLÍMACO', 'JARDIM DA GLÓRIA', 
             'JUSCELINO KUBITSCHECK', 'JORDANÉSIA', 'JUQUITIBA', 'LAPA', 'LEOPOLDINA', 'LIMÃO', 'LUBECA', 'MANDAQUI', 'MATEUS', 'MAUÁ', 'MONTE AZUL', 
             'MONTE BELO', 'MENINOS', 'MANOEL DA NÓBREGA', 'MONÇÕES', 'MONUMENTO', 'MOOCA', 'MORUMBI', 'MIGUEL PAULISTA', 'MONTE SANTO', 
             'NAÇÕES', 'NOVO MUNDO', 'ORATÓRIO', 'OSASCO', 'PAINEIRAS', 'PARNAIBA', 'PENHA NOVA', 'PERI', 'PIRAPORINHA', 'PQ DOS LAGOS', 'PINHEIROS', 
             'PONTA PORÃ', 'PONTE PRETA', 'PLANALTO PAULISTA', 'PARELHEIROS', 'PIRITUBA', 'PARQUE SÃO DOMINGOS', 'RIO BONITO', 'REMÉDIOS', 
             'RIO GRANDE', 'ROSELÂDIA', 'RIBEIRÃO PIRES', 'RAPOSO TAVARES', 'SACOMÃ', 'SANTO AMARO', 'SAÚDE', 'SÃO BERNARDO DO CAMPO', 'SERTÃOZINHO', 
             'SILVESTRE', 'SÃO JOAQUIM', 'SÃO LORENÇO', 'SANTA MARIA', 'SANTO ANDRÉ', 'SUMARÉ', 'TAIPAS', 'TAMBORÉ', 'TIRADENTES', 'TAMOIO', 
             'TENENTE MARQUES', 'TABOÃO DA SERRA', 'TUIUTI', 'TUCURUVI', 'UTINGA', 'VILA ALMEIDA', 'VARGINHA', 'VILA EMA', 'VILA FORMOSA', 
             'VARGEM GRANDE', 'VILA GUILHERME', 'VITORIA', 'VILA MARIANA', 'VILA MEDEIROS', 'VILA PAULA', 'VILA PAULICEIA', 'V.PROSPERIDADE', 
             'VILA TALARICO']
}

df = pd.DataFrame(data)

# Lista de bairros/cidades de São Paulo
bairros_sp = ['ALTO BOA VISTA', 'AMERICANÓPOLIS', 'ANCHIETA', 'BARRA FUNDA', 'BRÁS', 'BRASILÂNDIA', 'BUTANTÃ', 'CAMBUCI', 'CANINDÉ', 'CARRÃO',
              'CASA VERDE', 'CONGONHAS', 'CUPECÊ', 'ERMELINO MATARAZO', 'GOMES CARDIM', 'GRANJA JULIETA', 'GUAIANAZES', 'GUARAPIRANGA', 'GUMERCINDO',
              'HIPÓDROMO', 'IMIGRANTES', 'ITAIM', 'ITAQUERA', 'JAÇANÃ', 'JAGUARÉ', 'JOÃO CLÍMACO', 'JARDIM DA GLÓRIA', 'LAPA', 'LEOPOLDINA', 
              'LIMÃO', 'MANDAQUI', 'MOOCA', 'MORUMBI', 'ORATÓRIO', 'PARQUE SÃO DOMINGOS', 'PARELHEIROS', 'PENHA NOVA', 'PERI', 'PINHEIROS', 
              'PLANALTO PAULISTA', 'PIRITUBA', 'SACOMÃ', 'SANTO AMARO', 'SAÚDE', 'TAIPAS', 'TIRADENTES', 'TUCURUVI', 'VILA EMA', 'VILA FORMOSA',
              'VILA GUILHERME', 'VILA MARIANA', 'VILA MEDEIROS']

# Criando a coluna 'bairro_sp' com valor 1 para bairros de SP e 0 para os demais
df['bairro_sp'] = df['Nome'].apply(lambda x: 1 if x in bairros_sp else 0)

df.to_excel("database/subestacoes.xlsx",index=False)