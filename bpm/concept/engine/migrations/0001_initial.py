# -*- coding: utf-8 -*-
import datetime
from south.db import db
from south.v2 import SchemaMigration
from django.db import models


class Migration(SchemaMigration):

    def forwards(self, orm):
        # Adding model 'Defination'
        db.create_table(u'engine_defination', (
            (u'id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('name', self.gf('django.db.models.fields.SlugField')(max_length=255)),
            ('category', self.gf('django.db.models.fields.PositiveSmallIntegerField')()),
            ('content', self.gf('django.db.models.fields.TextField')()),
        ))
        db.send_create_signal(u'engine', ['Defination'])

        # Adding model 'Task'
        db.create_table(u'engine_task', (
            (u'id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('args', self.gf('django.db.models.fields.TextField')()),
            ('kwargs', self.gf('django.db.models.fields.TextField')()),
            ('is_complete', self.gf('django.db.models.fields.BooleanField')(default=False)),
            ('is_confirmed', self.gf('django.db.models.fields.BooleanField')(default=False)),
            ('process', self.gf('django.db.models.fields.related.ForeignKey')(related_name=u'process', to=orm['engine.Process'])),
            ('result', self.gf('django.db.models.fields.TextField')()),
        ))
        db.send_create_signal(u'engine', ['Task'])

        # Adding model 'Process'
        db.create_table(u'engine_process', (
            (u'id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('defination', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['engine.Defination'])),
            ('task', self.gf('django.db.models.fields.related.ForeignKey')(related_name=u'task', to=orm['engine.Task'])),
            ('state', self.gf('django.db.models.fields.PositiveSmallIntegerField')(default=0)),
            ('is_locked', self.gf('django.db.models.fields.BooleanField')(default=False)),
            ('pickled', self.gf('django.db.models.fields.TextField')(db_column='pickle')),
        ))
        db.send_create_signal(u'engine', ['Process'])


    def backwards(self, orm):
        # Deleting model 'Defination'
        db.delete_table(u'engine_defination')

        # Deleting model 'Task'
        db.delete_table(u'engine_task')

        # Deleting model 'Process'
        db.delete_table(u'engine_process')


    models = {
        u'engine.defination': {
            'Meta': {'object_name': 'Defination'},
            'category': ('django.db.models.fields.PositiveSmallIntegerField', [], {}),
            'content': ('django.db.models.fields.TextField', [], {}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'name': ('django.db.models.fields.SlugField', [], {'max_length': '255'})
        },
        u'engine.process': {
            'Meta': {'object_name': 'Process'},
            'defination': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['engine.Defination']"}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'is_locked': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'pickled': ('django.db.models.fields.TextField', [], {'db_column': "'pickle'"}),
            'state': ('django.db.models.fields.PositiveSmallIntegerField', [], {'default': '0'}),
            'task': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "u'task'", 'to': u"orm['engine.Task']"})
        },
        u'engine.task': {
            'Meta': {'object_name': 'Task'},
            'args': ('django.db.models.fields.TextField', [], {}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'is_complete': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'is_confirmed': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            'kwargs': ('django.db.models.fields.TextField', [], {}),
            'process': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "u'process'", 'to': u"orm['engine.Process']"}),
            'result': ('django.db.models.fields.TextField', [], {})
        }
    }

    complete_apps = ['engine']