import { general_error } from './error.js';
let gpus_popup = false;
let models_data = null;

function get_gpus() {
    fetch("/tab-host-have-gpus")
    .then(function(response) {
        return response.json();
    })
    .then(function(data) {
        render_gpus(data);
    })
   .catch(function(error) {
        console.log('tab-host-have-gpus',error);
        general_error(error);
    });
}

function render_gpus(gpus) {
    if(gpus_popup) { return; }
    if(gpus.gpus.length == 0) {
        document.querySelector('.gpus-pane').style.display = 'none';
    } else {
        document.querySelector('.gpus-pane').style.display = 'div';
    }
    const gpus_list = document.querySelector('.gpus-list');
    gpus_list.innerHTML = '';
    gpus.gpus.forEach(element => {
        const row = document.createElement('div');
        row.classList.add('gpus-item');
        row.setAttribute('gpu',element.id);
        const gpu_wrapper = document.createElement('div');
        gpu_wrapper.classList.add('gpus-content');
        const gpu_name = document.createElement("h3");
        gpu_name.classList.add('gpus-title');
        const gpu_image = document.createElement("div");
        gpu_image.classList.add('gpus-card');
        const gpu_mem = document.createElement("div");
        gpu_mem.classList.add('gpus-mem');
        const gpu_temp = document.createElement("div");
        gpu_temp.classList.add('gpus-temp');
        const used_gb = format_memory(element.mem_used_mb);
        const total_gb = format_memory(element.mem_total_mb);
        const used_mem = Math.round(element.mem_used_mb / (element.mem_total_mb / 100));
        gpu_name.innerHTML = element.name;
        gpu_mem.innerHTML = `<b>Mem</b><div class="gpus-mem-wrap"><div class="gpus-mem-bar"><span style="width: ${used_mem}%"></span></div>${used_gb}/${total_gb} GB</div>`;
        if (element.temp_celsius < 0) {
            gpu_temp.innerHTML = `<b>Temp</b> N/A`;
        } else {
            gpu_temp.innerHTML = `<b>Temp</b>` + element.temp_celsius + '°C';
        }
        row.appendChild(gpu_image);
        gpu_wrapper.appendChild(gpu_name);
        gpu_wrapper.appendChild(gpu_mem);
        gpu_wrapper.appendChild(gpu_temp);
        element.statuses.forEach(status => {
            const gpu_command = document.createElement("div");
            gpu_command.classList.add('gpus-command');
            const gpu_status = document.createElement("div");
            gpu_status.classList.add('gpus-status');
            gpu_command.innerHTML = `<span class="gpus-current-status">${status.status}</span>`;
            gpu_status.innerHTML += `<div><b>Command</b>${status.command}</div>`;
            gpu_status.innerHTML += `<div><b>Status</b>${status.status}</div>`;
            gpu_command.appendChild(gpu_status);
            gpu_command.addEventListener('mouseover',function(e) {
                gpus_popup = true;
                this.querySelector('.gpus-status').classList.add('gpus-status-visible');
            });
            gpu_command.addEventListener('mouseout',function(e) {
                gpus_popup = false;
                this.querySelector('.gpus-status').classList.remove('gpus-status-visible');
            });
            if(!status.status || status.status === '') {
                gpu_command.classList.add('gpus-status-invisible');
            }
            gpu_wrapper.appendChild(gpu_command);
        });

        row.appendChild(gpu_wrapper);
        gpus_list.appendChild(row);
    });
}

function get_models()
{
    fetch("/tab-host-models-get")
    .then(function(response) {
        return response.json();
    })
    .then(function(data) {
        models_data = data;
        render_models_assigned(data.model_assign);
        const enable_chat_gpt_switch = document.getElementById('enable_chat_gpt');
        enable_chat_gpt_switch.removeEventListener('change', save_model_assigned);
        enable_chat_gpt_switch.checked = models_data['openai_api_enable'];
        enable_chat_gpt_switch.addEventListener('change', save_model_assigned);
        const more_gpus_notification = document.querySelector('.model-hosting-error');
        if(data.hasOwnProperty('more_models_than_gpus') && data.more_models_than_gpus) {
            more_gpus_notification.classList.remove('d-none');
        } else {
            more_gpus_notification.classList.add('d-none');
        }
        const required_memory_exceed_available = document.querySelector('.model-memory-error');
        if(data.hasOwnProperty('required_memory_exceed_available') && data.required_memory_exceed_available) {
            required_memory_exceed_available.classList.remove('d-none');
        } else {
            required_memory_exceed_available.classList.add('d-none');
        }
    })
    .catch(function(error) {
        console.log('tab-host-models-get',error);
        general_error(error);
    });
}

function save_model_assigned() {
    let openai_enable = document.querySelector('#enable_chat_gpt');
    const data = {
        model_assign: {
            ...models_data.model_assign,
        },
        completion: models_data.completion ? models_data.completion : "",
        openai_api_enable: openai_enable.checked
    };
    fetch("/tab-host-models-assign", {
        method: "POST",
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(data)
    })
    .then(function (response) {
        get_models();
    })
   .catch(function (error) {
        console.log('tab-host-models-assign',error);
        general_error(error);
    });
}

function render_models_assigned(models) {
    const models_table = document.querySelector('.table-assigned-models tbody');
    models_table.innerHTML = '';
    for(let index in models) {
        const row = document.createElement('tr');
        row.setAttribute('data-model',index);
        const model_name = document.createElement("td");
        const completion = document.createElement("td");
        const finetune_info = document.createElement("td");
        const select_gpus = document.createElement("td");
        const gpus_share = document.createElement("td");
        const del = document.createElement("td");

        let quantization_html = "";
        if (models[index].spec.quantization) {
            quantization_html = `
                <div class="text-secondary">Quantization: ${models[index].spec.quantization}</div>
            `;
        }
        model_name.innerHTML = `
            <div>${index}</div>
            <div class="text-secondary"><div>Backend: ${models[index].spec.backend}</div>
            ${quantization_html}
        `;

        const model_info = models_data.models.filter((item) => item.spec_id === models[index].spec_id)[0];
        if (model_info.has_completion) {
            const completion_input = document.createElement("input");
            completion_input.setAttribute('type','radio');
            completion_input.setAttribute('name','completion-radio-button');
            completion_input.setAttribute('value',index);
            if (models_data.completion === index) {
                completion_input.checked = true;
            }
            completion_input.setAttribute('model',index);
            completion_input.addEventListener('change', function() {
                models_data.completion = this.value;
                save_model_assigned();
            });
            completion.appendChild(completion_input);
        }

        if (model_info.finetune_info) {
            finetune_info.innerHTML = `
            <table cellpadding="5">
                <tr>
                    <td>Run: </td>
                    <td>${model_info.finetune_info.run}</td>
                </tr>
                <tr>
                    <td>Checkpoint: </td>
                    <td>${model_info.finetune_info.checkpoint}</td>
                </tr>
            </table>
            `;
        }

         if (model_info.has_sharding) {
            const select_gpus_div = document.createElement("div");
            select_gpus_div.setAttribute("class", "btn-group btn-group-sm");
            select_gpus_div.setAttribute("role", "group");
            select_gpus_div.setAttribute("aria-label", "basic radio toggle button group");

            [1, 2, 4].forEach((gpus_shard_n) => {
                const input_name = `gpu-${index}`;
                const input_id = `${input_name}-${gpus_shard_n}`;

                const input = document.createElement("input");
                input.setAttribute("type", "radio");
                input.setAttribute("class", "gpu-switch btn-check");
                input.setAttribute("name", input_name);
                input.setAttribute("id", input_id);
                if (models_data.model_assign[index].gpus_shard === gpus_shard_n) {
                    input.checked = true;
                }

                const label = document.createElement("label");
                label.setAttribute("class", "btn btn-outline-primary");
                label.setAttribute("for", input_id);
                label.innerHTML = gpus_shard_n;

                input.addEventListener('change', () => {
                    models_data.model_assign[index].gpus_shard = gpus_shard_n;
                    save_model_assigned();
                });

                select_gpus_div.appendChild(input);
                select_gpus_div.appendChild(label);
            });
            select_gpus.appendChild(select_gpus_div);
        }

        const gpus_checkbox = document.createElement("input");
        gpus_checkbox.setAttribute('type','checkbox');
        gpus_checkbox.setAttribute('value',index);
        gpus_checkbox.setAttribute('name',`share-${index}`);
        gpus_checkbox.classList.add('form-check-input');
        if(models_data.model_assign[index].share_gpu) {
            gpus_checkbox.checked = true;
        } 
        gpus_checkbox.addEventListener('change', function() {
            if(this.checked) {
                models_data.model_assign[index].share_gpu = true;
            } else {
                models_data.model_assign[index].share_gpu = false;
            }
            save_model_assigned();
        });
        gpus_share.appendChild(gpus_checkbox);

        const del_button = document.createElement("button");
        del_button.innerHTML = `<i class="bi bi-trash3-fill"></i>`;
        del_button.dataset.model = index;
        del_button.addEventListener('click', function() {
            delete models_data.model_assign[index];
            save_model_assigned();
        });
        del_button.classList.add('model-remove','btn','btn-danger');
        del.appendChild(del_button);

        row.appendChild(model_name);
        row.appendChild(completion);
        row.appendChild(finetune_info);
        row.appendChild(select_gpus);
        row.appendChild(gpus_share);
        row.appendChild(del);
        models_table.appendChild(row);
    }
}

function render_models(models) {
    const models_table = document.querySelector('.table-models tbody');
    models_table.innerHTML = '';

    const unique_key = (backend, quantization) => `${backend}-${quantization}`;
    const grouped_models = models.models.reduce((accumulator, item) => {
        const model_name = item.name;
        if (!accumulator[model_name]) {
            accumulator[model_name] = {};
        }
        const key = unique_key(item.spec.backend, item.spec.quantization);
        if (accumulator[model_name].hasOwnProperty(key)) {
            console.log(
                "multiple specs with the same backend and quantization",
                model_name, item.spec.backend, item.spec.quantization);
        } else {
            accumulator[model_name][key] = item;
        }
        return accumulator;
    }, {});

    Object.entries(grouped_models)
          .sort((a, b) => a[0].localeCompare(b[0], undefined, { sensitivity: 'base' }))
          .forEach(([m_name, m_items]) => {
        const row = document.createElement('tr');
        row.setAttribute('data-model', m_name);

        const model_name = document.createElement("td");
        const has_completion = document.createElement("td");
        const has_finetune = document.createElement("td");
        const has_toolbox = document.createElement("td");
        const has_chat = document.createElement("td");
        const backend = document.createElement("td");
        const quantization = document.createElement("td");

        model_name.textContent = m_name;

        const default_item = Object.values(m_items).filter((item) => item.spec.default)[0];
        const default_backend = default_item.spec.backend;
        const default_quantization = default_item.spec.quantization;

        const backend_combobox = document.createElement("select");
        backend_combobox.classList.add('form-select');
        const backend_set = new Set(Object.values(m_items).reduce((accumulator, item) => {
            accumulator.push(item.spec.backend);
            return accumulator;
        }, []));
        backend_set.forEach((item) => {
            const option = document.createElement('option');
            option.text = item;
            option.value = item;
            option.selected = (item == default_backend);
            backend_combobox.add(option);
        });
        backend.appendChild(backend_combobox);

        const quantization_combobox = document.createElement("select");
        quantization_combobox.classList.add('form-select');
        quantization.appendChild(quantization_combobox);

        const selected_item = () => {
            const key = unique_key(backend_combobox.value, quantization_combobox.value);
            return m_items[key];
        };
        const render_model_row = function() {
            const selected_quantization = quantization_combobox.value ? quantization_combobox.value : default_quantization;
            while (quantization_combobox.options.length > 0) {
                quantization_combobox.remove(0);
            }
            const quantization_set = new Set(Object.values(m_items).reduce((accumulator, item) => {
                if (item.spec.backend == backend_combobox.value) {
                    accumulator.push(item.spec.quantization);
                }
                return accumulator;
            }, []));
            quantization_set.forEach((item) => {
                const option = document.createElement('option');
                option.text = item ? item : 'auto';
                option.value = item;
                option.selected = (item == selected_quantization);
                quantization_combobox.add(option);
            });
            const s_item = selected_item();
            has_completion.innerHTML = s_item.has_completion ? '<i class="bi bi-check"></i>' : '';
            has_finetune.innerHTML = s_item.has_finetune ? '<i class="bi bi-check"></i>' : '';
            has_toolbox.innerHTML = s_item.has_toolbox ? '<i class="bi bi-check"></i>' : '';
            has_chat.innerHTML = s_item.has_chat ? '<i class="bi bi-check"></i>' : '';
        }
        backend.addEventListener('click', (event) => {
            event.stopPropagation();
            render_model_row();
        });
        quantization.addEventListener('click', (event) => {
            event.stopPropagation();
            render_model_row();
        });
        render_model_row();

        row.appendChild(model_name);
        row.appendChild(has_completion);
        row.appendChild(has_finetune);
        row.appendChild(has_toolbox);
        row.appendChild(has_chat);
        row.appendChild(backend);
        row.appendChild(quantization);
        models_table.appendChild(row);
        row.addEventListener('click', function(e) {
            models_data.model_assign[m_name] = {
                gpus_shard: 1,
                share_gpu: false,
                spec: selected_item().spec,
            };
            save_model_assigned();
            const add_model_modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('add-model-modal'));
            add_model_modal.hide();
        });
    });
}

function format_memory(memory_in_mb, decimalPlaces = 2) {
    const memory_in_gb = (memory_in_mb / 1024).toFixed(decimalPlaces);
    return memory_in_gb;
}

export async function init(general_error) {
    let req = await fetch('/tab-model-hosting.html');
    document.querySelector('#model-hosting').innerHTML = await req.text();
    get_gpus();
    get_models();
    const add_model_modal = document.getElementById('add-model-modal');
    add_model_modal.addEventListener('show.bs.modal', function () {
        render_models(models_data);
    });
    const redirect2credentials = document.getElementById('redirect2credentials');
    redirect2credentials.addEventListener('click', function() {
        document.querySelector(`[data-tab=${redirect2credentials.getAttribute('data-tab')}]`).click();
    });
    // const enable_chat_gpt_switch = document.getElementById('enable_chat_gpt');
}

export function tab_switched_here() {
    get_gpus();
    get_models();
}

export function tab_switched_away() {
}

export function tab_update_each_couple_of_seconds() {
    get_gpus();
}
