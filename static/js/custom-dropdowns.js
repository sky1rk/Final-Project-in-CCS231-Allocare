document.addEventListener('DOMContentLoaded', () => {
    const selectElements = Array.from(document.querySelectorAll('select'));

    const closeAll = (exceptDropdown = null) => {
        document.querySelectorAll('.custom-dropdown.is-open').forEach((dropdown) => {
            if (dropdown !== exceptDropdown) {
                dropdown.classList.remove('is-open');
            }
        });
    };

    selectElements.forEach((selectElement) => {
        if (selectElement.dataset.customDropdown === 'ready') {
            return;
        }

        const wrapper = document.createElement('div');
        wrapper.className = 'custom-dropdown';

        const trigger = document.createElement('button');
        trigger.type = 'button';
        trigger.className = 'custom-dropdown-trigger';
        trigger.setAttribute('aria-haspopup', 'listbox');
        trigger.setAttribute('aria-expanded', 'false');

        const valueLabel = document.createElement('span');
        valueLabel.className = 'custom-dropdown-value';

        const caret = document.createElement('span');
        caret.className = 'custom-dropdown-caret';
        caret.setAttribute('aria-hidden', 'true');
        caret.innerHTML = '<svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 7l5 6 5-6"></path></svg>';

        const menu = document.createElement('div');
        menu.className = 'custom-dropdown-menu';
        menu.setAttribute('role', 'listbox');

        const selectName = selectElement.name || selectElement.id || 'dropdown';
        const menuId = `${selectName}-menu-${Math.random().toString(36).slice(2, 8)}`;
        menu.id = menuId;
        trigger.setAttribute('aria-controls', menuId);

        const syncTriggerLabel = () => {
            const selectedOption = selectElement.selectedOptions[0];
            const text = selectedOption ? selectedOption.textContent.trim() : '';
            const hasPlaceholder = selectedOption && selectedOption.value === '';
            valueLabel.textContent = text || selectElement.getAttribute('data-placeholder') || 'Select an option';
            wrapper.classList.toggle('has-value', !hasPlaceholder && Boolean(text));
            menu.querySelectorAll('.custom-dropdown-option').forEach((option) => {
                option.classList.toggle('is-selected', option.getAttribute('data-value') === selectElement.value);
                option.setAttribute('aria-selected', option.getAttribute('data-value') === selectElement.value ? 'true' : 'false');
            });
        };

        Array.from(selectElement.options).forEach((optionElement) => {
            const optionButton = document.createElement('button');
            optionButton.type = 'button';
            optionButton.className = 'custom-dropdown-option';
            optionButton.setAttribute('role', 'option');
            optionButton.setAttribute('data-value', optionElement.value);
            optionButton.textContent = optionElement.value ? optionElement.textContent.trim() : optionElement.textContent.trim();
            optionButton.disabled = optionElement.disabled;
            optionButton.addEventListener('click', () => {
                selectElement.value = optionElement.value;
                selectElement.dispatchEvent(new Event('change', { bubbles: true }));
                syncTriggerLabel();
                wrapper.classList.remove('is-open');
                trigger.setAttribute('aria-expanded', 'false');
                trigger.focus();
            });
            menu.appendChild(optionButton);
        });

        trigger.addEventListener('click', () => {
            const isOpen = wrapper.classList.contains('is-open');
            closeAll(wrapper);
            wrapper.classList.toggle('is-open', !isOpen);
            trigger.setAttribute('aria-expanded', String(!isOpen));
        });

        trigger.addEventListener('keydown', (event) => {
            if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                trigger.click();
            }
            if (event.key === 'Escape') {
                wrapper.classList.remove('is-open');
                trigger.setAttribute('aria-expanded', 'false');
            }
        });

        menu.addEventListener('keydown', (event) => {
            if (event.key === 'Escape') {
                wrapper.classList.remove('is-open');
                trigger.setAttribute('aria-expanded', 'false');
                trigger.focus();
            }
        });

        selectElement.addEventListener('change', syncTriggerLabel);
        selectElement.addEventListener('focus', () => wrapper.classList.add('is-focused'));
        selectElement.addEventListener('blur', () => wrapper.classList.remove('is-focused'));

        selectElement.dataset.customDropdown = 'ready';
        selectElement.classList.add('select-native');
        selectElement.setAttribute('aria-hidden', 'true');
        selectElement.tabIndex = -1;

        selectElement.parentNode.insertBefore(wrapper, selectElement);
        wrapper.appendChild(selectElement);
        wrapper.appendChild(trigger);
        trigger.appendChild(valueLabel);
        trigger.appendChild(caret);
        wrapper.appendChild(menu);

        const placeholderOption = Array.from(selectElement.options).find((optionElement) => optionElement.value === '');
        if (placeholderOption) {
            selectElement.setAttribute('data-placeholder', placeholderOption.textContent.trim());
        }
        syncTriggerLabel();
    });

    document.addEventListener('click', (event) => {
        const target = event.target;
        if (!target.closest('.custom-dropdown')) {
            closeAll();
        }
    });

    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
            closeAll();
        }
    });

    document.addEventListener('focusin', (event) => {
        if (!event.target.closest('.custom-dropdown')) {
            closeAll();
        }
    });
});
