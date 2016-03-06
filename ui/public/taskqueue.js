$(function() {

  if (window['WebSocket']) {
    $('#warning').hide();

    var topics = [];

    function openConnection() {
      var url = '';
      if (window.location.protocol === 'https:') {
          url = 'wss:';
      } else {
          url = 'ws:';
      }
      url = url + '//' + window.location.host + '/ws';

      var conn = new WebSocket(url);
      conn.onopen = function(evt) {
        $('#CONNECTED').show();
        $('#DISCONNECTED').hide();
      }
      conn.onclose = function(evt) {
        $('#CONNECTED').hide();
        $('#DISCONNECTED').show();
        setTimeout(openConnection(), 5000);
      }

      conn.onmessage = function(evt) {
        $('#CONNECTED').show();
        $('#DISCONNECTED').hide();
        if (!evt.data) {
          return;
        }
        var e = JSON.parse(evt.data);
        if (e.task) {
          var topic = e.task.topic;
          if (topics.indexOf(e.task.topic) === -1) {
            topics.push(e.task.topic);
            $('#TOPICS').append('<li class="list-group-item">' + topic + '</li>');
          }
        }
        switch (e.type) {
          case 'MANAGER_START':
            $('#MANAGER_STATE').html('Manager started');
            break;
          case 'MANAGER_STOP':
            $('#MANAGER_STATE').html('Manager stopped');
            break;
          case 'MANAGER_STATS':
            var stats = e.stats;
            if (stats) {
              $('#STATS_ENQUEUED').html(stats.enqueued || 0);
              $('#STATS_STARTED').html(stats.started || 0);
              $('#STATS_COMPLETED').html(stats.completed || 0);
              $('#STATS_RETRIED').html(stats.retried || 0);
              $('#STATS_FAILED').html(stats.failed || 0);
              $('#INPUT_QUEUE_SIZE').html(stats.input_queue_size || 0);
              $('#WORK_QUEUE_SIZE').html(stats.work_queue_size || 0);
              $('#DEAD_QUEUE_SIZE').html(stats.dead_queue_size || 0);
            }
            break;
          case 'TASK_START':
            var count = parseInt($('#TASK_START').html());
            count += 1;
            $('#TASK_START').html(count);
            break;
          case 'TASK_RETRY':
            var count = parseInt($('#TASK_RETRY').html());
            count += 1;
            $('#TASK_RETRY').html(count);
            break;
          case 'TASK_COMPLETION':
            var count = parseInt($('#TASK_COMPLETION').html());
            count += 1;
            $('#TASK_COMPLETION').html(count);
            break;
          case 'TASK_FAILURE':
            var count = parseInt($('#TASK_FAILURE').html());
            count += 1;
            $('#TASK_FAILURE').html(count);
            break;
        }
      }
    }
    openConnection();

  } else {
    $('#warning').show();
  }
});
