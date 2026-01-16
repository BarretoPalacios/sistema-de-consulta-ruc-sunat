// Funciones generales para la aplicación

document.addEventListener('DOMContentLoaded', function() {
    // Inicializar tooltips de Bootstrap
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
    
    // Validación de formularios
    const forms = document.querySelectorAll('form');
    forms.forEach(form => {
        form.addEventListener('submit', function(e) {
            const submitBtn = this.querySelector('button[type="submit"]');
            if (submitBtn) {
                submitBtn.disabled = true;
                submitBtn.innerHTML = '<span class="loading-spinner"></span> Procesando...';
            }
        });
    });
    
    // Auto completar RUC
    const rucInput = document.getElementById('ruc');
    if (rucInput) {
        rucInput.addEventListener('input', function(e) {
            let value = this.value.replace(/\D/g, '');
            if (value.length > 11) value = value.substring(0, 11);
            this.value = value;
        });
        
        rucInput.addEventListener('keypress', function(e) {
            if (!/\d/.test(e.key)) {
                e.preventDefault();
            }
        });
    }
    
    // Cargar estadísticas en tiempo real
    if (window.location.pathname === '/' || window.location.pathname === '/estadisticas') {
        cargarEstadisticas();
    }
    
    // Manejar notificaciones
    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.has('success')) {
        mostrarNotificacion(urlParams.get('message') || 'Operación exitosa', 'success');
    }
    if (urlParams.has('error')) {
        mostrarNotificacion(urlParams.get('message') || 'Ocurrió un error', 'danger');
    }
});

function cargarEstadisticas() {
    fetch('/api/v1/estadisticas')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                actualizarUIEstadisticas(data.data);
            }
        })
        .catch(error => console.error('Error cargando estadísticas:', error));
}

function actualizarUIEstadisticas(stats) {
    // Actualizar elementos con datos estadísticos
    const elements = {
        'total-consultas': stats.estadisticas.total_consultas,
        'tasa-exito': stats.estadisticas.tasa_exito,
        'tiempo-promedio': stats.estadisticas.tiempo_promedio_ms,
        'total-registros': stats.base_datos.total_registros
    };
    
    for (const [id, value] of Object.entries(elements)) {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = value;
        }
    }
}

function mostrarNotificacion(mensaje, tipo = 'info') {
    const alertHTML = `
        <div class="alert alert-${tipo} alert-dismissible fade show" role="alert">
            ${mensaje}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
    
    // Agregar al inicio del main
    const main = document.querySelector('main');
    if (main) {
        main.insertAdjacentHTML('afterbegin', alertHTML);
        
        // Auto cerrar después de 5 segundos
        setTimeout(() => {
            const alert = document.querySelector('.alert');
            if (alert) {
                bootstrap.Alert.getInstance(alert).close();
            }
        }, 5000);
    }
}

// Funciones de exportación
function exportarDatos(ruc, formato) {
    window.location.href = `/exportar/descargar?ruc=${ruc}&formato=${formato}`;
}

// Funciones de búsqueda rápida
function buscarRapido() {
    const ruc = document.getElementById('ruc-rapido').value;
    if (ruc && ruc.length === 11) {
        window.location.href = `/buscar/resultado?ruc=${ruc}`;
    } else {
        mostrarNotificacion('Por favor ingrese un RUC válido de 11 dígitos', 'warning');
    }
}

// Manejo del historial
function cargarHistorial() {
    const historial = JSON.parse(localStorage.getItem('ruc_historial') || '[]');
    const container = document.getElementById('historial-container');
    
    if (!container) return;
    
    if (historial.length === 0) {
        container.innerHTML = `
            <div class="text-center text-muted py-4">
                <i class="bi bi-clock-history display-4"></i>
                <p class="mt-2">No hay búsquedas en el historial</p>
            </div>
        `;
        return;
    }
    
    let html = '<div class="list-group">';
    historial.forEach((item, index) => {
        html += `
            <div class="list-group-item list-group-item-action">
                <div class="d-flex w-100 justify-content-between">
                    <h6 class="mb-1">${item.nombre || 'Sin nombre'}</h6>
                    <small>${new Date(item.fecha).toLocaleDateString()}</small>
                </div>
                <p class="mb-1">
                    <span class="badge bg-primary">${item.ruc}</span>
                    <span class="badge ${item.data.estado_contribuyente === 'ACTIVO' ? 'bg-success' : 'bg-warning'}">
                        ${item.data.estado_contribuyente || 'N/A'}
                    </span>
                </p>
                <small class="text-muted">${item.data.direccion_simple || 'Sin dirección'}</small>
                <div class="mt-2">
                    <button onclick="verDetalle('${item.ruc}')" class="btn btn-sm btn-outline-primary">
                        <i class="bi bi-eye"></i> Ver
                    </button>
                    <button onclick="exportarDesdeHistorial('${item.ruc}', 'json')" class="btn btn-sm btn-outline-success">
                        <i class="bi bi-download"></i> JSON
                    </button>
                </div>
            </div>
        `;
    });
    html += '</div>';
    container.innerHTML = html;
}

function verDetalle(ruc) {
    window.location.href = `/buscar/resultado?ruc=${ruc}`;
}

function exportarDesdeHistorial(ruc, formato) {
    exportarDatos(ruc, formato);
}

// Inicializar historial si estamos en la página de estadísticas
if (window.location.pathname === '/estadisticas') {
    document.addEventListener('DOMContentLoaded', cargarHistorial);
}