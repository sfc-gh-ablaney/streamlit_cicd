conda create --name streamlit_venv python=3.10
conda activate streamlit_venv
pip install -r requirements.txt

streamlit run streamlit_app.py


conda deactivate
conda remove -n streamlit_venv --all