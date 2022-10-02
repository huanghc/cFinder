VENV = venv

install: clean pull_app_code set_python_env
run_all: run run_history

set_python_env:
	@echo "\n** Step 2: Set CFinder Python Envirnment **\n"
	$(shell which python3) -m venv $(VENV)
	$(VENV)/bin/pip install -r requirements.txt 
    
pull_app_code:
	@echo "\n** Step 1: Pull application code **\n"
	@mkdir -p app_code;
	@cd app_code; git clone https://github.com/django-oscar/django-oscar.git; cd django-oscar; git checkout 48cc5c2e6
	@cd app_code; git clone https://github.com/saleor/saleor.git; cd saleor; git checkout 53e519df6
	@cd app_code; git clone https://github.com/zulip/zulip.git; cd zulip; git checkout f5bb43aba23b
	@cd app_code; git clone https://github.com/wagtail/wagtail.git; cd wagtail; git checkout 317f100a7
	@cd app_code; git clone https://github.com/openedx/edx-platform.git; cd edx-platform; git checkout 97edc47
	@cd app_code; git clone https://github.com/openedx/ecommerce.git; cd ecommerce; git checkout 27e6b06b
	@cd app_code; git clone https://github.com/shuup/shuup.git; cd shuup; git checkout 25f78cf

clean:
	@echo "\n** Step 0: Clean the environment, result, app code **\n"
	@rm -rf venv
	@rm -rf app_code
	@rm -rf result
	@rm -rf result_history_issues

run:
	@echo "\n** Step 3: Run CFinder - static analysis **\n"
	@mkdir -p result;
	. venv/bin/activate; python pattern_finder.py

run_history:
	@echo "\n** Step 4: Run CFinder on history issues **\n"
	@mkdir -p result_history_issues;
	. venv/bin/activate; python pattern_finder_history_issue.py
