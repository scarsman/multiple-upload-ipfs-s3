from orator.migrations import Migration


class CreateTasksTable(Migration):

    def up(self):
        """
        Run the migrations.
        """
        with self.schema.create('tasks') as table:
            table.increments('id')
            table.text("image_path")
            table.text("ipfs_hash").unsigned().default('')
            table.integer("processing_status").unsigned().default(0)            
            table.timestamps()

    def down(self):
        """
        Revert the migrations.
        """
        self.schema.drop('tasks')
